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

(defn partition-by-delay [dispatch msecs input-chan]
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
