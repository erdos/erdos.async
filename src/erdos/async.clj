(ns erdos.async
  (:require [clojure.core.async :refer [go chan <! >!] :as async]
            [clojure.core.async.impl.protocols :as impl]))

;(def)

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

;; TODO: impl this
(defn directory-watch-chan
  "Returns a channel of file system change events for a given directory."
  [directory]
  (let []
    (reify
      impl/Channel
      (closed? [_] false)
      (close! [_] nil)
      impl/ReadPort
      (take! [_ fn1]
        nil
        ))))

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
