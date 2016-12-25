(ns erdos.async
  (:require [clojure.core.async :refer [go chan <! >!] :as async]
            [clojure.core.async.impl.protocols :as impl]))

(defmacro go-try
  "A go block that wraps exceptions and returns them as values."
  [& bodies]
  `(go (try ~@bodies
            (catch Throwable t# t#))))

(defmacro <?
  "A take macro that throws Throwable values."
  [c]
  `(let [r# (<! ~c)]
     (if (instance? java.lang.Throwable r#) (throw r#) r#)))


(defn fn-chan [body-fn]
  (let [end? (atom false)]
    (reify
      impl/Channel
      (close! [_] (reset! end? true))
      (closed? [_] @end?)
      impl/ReadPort
      (take! [_ fn1]
        (when-not @end?
          (->
           (go (let [result (body-fn)]
                 (when (nil? result) (reset! end? true))
                 result))
           (impl/take! fn1)))))))


(defmacro go-fn
  "Createas a go block that evaluates its body on reads.
   Once it return nil it remains closed and body is never invoked again.
   Not thread-safe."
  [& bodies] `(fn-chan (fn [] ~@bodies)))


(defmacro go-promise
  "A wrapper macro around promise-chan."
  [& bodies]
  `(doto (async/promise-chan)
     (-> (<! (do ~@bodies)) (go))))


(defmacro go-delay
  "Creates a go block that evaluates its body on first take.
   All subsequent reads will return the result of the first evaluation."
  [& bodies]
  `(let [c# (delay (go ~@bodies))]
     (go-fn (<! @c#))))

(defn repeatedly-chan
  "Like repeatedly but returns a channel that is closed on the first nil value."
  [fn1]
  (let [c (chan)]
    (go (loop []
          (if-some [x (fn1)]
            (do (>! c x)
                (recur))
            (async/close! c))))
    c))

;; TODO: impl this
(defn directory-watch-chan
  "Returns a channel of file system change events for a given directory."
  ([directory] (directory-watch-chan directory 1))
  ([directory buffer-size]
   (let [
         fs (java.io.file.FileSystems/getDefault)
         ws (.newWatchService fs)
         watchable ^java.nio.file.Watchable (dir->watchange directory)
         events (chan buffer-size)
         evts [java.nio.file.StandardWatchEventKinds/ENTRY_CREATE
               java.nio.file.StandardWatchEventKinds/ENTRY_DELETE
               java.nio.file.StandardWatchEventKinds/ENTRY_MODIFY]
         watch-key (.register watchable (to-array evts))
         parse-evt (fn [^java.io.file.WatchEvent evt]
                     (bean evt))]
     ;; elindithatunk egy szalat csak erre a celra.
     ;; close eseten elzarjuk

     ;; on stop: call watch-key.cancel

     ;; thread events to channel
     (async/thread
       (doseq [events (repeatedly #(.pollEvents watch-key))
               evt    events
               :while (.isValid watch-key)]
         (async/>!! events (parse-evt x)))
       (async/close! events))

     (reify
       impl/Channel
       (closed? [_] (.isValid watch-key))
       (close! [_]  (.cancel watch-key))
       impl/ReadPort
       (take! [_ fn1]
         (impl/take! events fn1))))))


;; example usage:
;; (partition-by-delay :file-name 5000 file-change-channel)
;; TODO: impl dispatch usage
(defn partition-by-delay [dispatch-fn msecs input-chan]
  (let [output (async/chan)]
    (async/go-loop []
      (if-let [x (<! input-chan)]
        (do
          (loop [coll [x]] ;; create a queue
            (let [[x c] (async/alts! [input-chan (async/timeout msecs)])]
              (if (= c input-chan)
                (if x
                  (recur (conj coll x))
                  (do (>! output coll)
                      (async/close! output)))
                (>! output coll))))
          (recur))
        (async/close! output)))
    output))


(defn partition-delay [msecs input-chan]
  (let [output (async/chan)]
    (async/go-loop []
      (if-let [x (<! input-chan)]
        (do
          (loop [coll [x]] ;; create a queue
            (let [[x c] (async/alts! [input-chan (async/timeout msecs)])]
              (if (= c input-chan)
                (if x
                  (recur (conj coll x))
                  (do (>! output coll)
                      (async/close! output)))
                (>! output coll))))
          (recur))
        (async/close! output)))
    output))


(defn directory-watch-delayed-chan
  "Channel of file names that are changed in a directory (delayed by 5000 ms)"
  [directory]
  (->> (file-watcher-channel directory)
       (partition-by-delay (comp :file-name first) 5000)))


(defmacro dochan
  "Like (doseq) for channels. Returns a channel that is closed after finished iteration."
  [[k c :as kvs] & bodies]
  (assert (= 2 (count kvs)))
  `(let [result# (chan)]
     (go (loop []
           (if-let [~k (<! ~c)]
             (do ~@bodies (recur))
             (async/close! result))))
     result#))
