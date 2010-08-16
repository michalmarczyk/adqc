(ns adqc.lqp.translate
  (:use adqc.lqp.operators))

(defprotocol ADQCtoDQP
  (adqc2dqp [operator]))

#_
(defmacro new-dqp [dqp-op & ctor-args]
  ;; TODO: check the package name & whether all DQP ops share it
  `(new ~(symbol (str "uk.org.ogsadai.dqp.operators." dqp-op))
        ~@ctor-args))

#_
(extend-protocol-to-operators ADQCtoDQP
  root
  (adqc2dqp [root]
    (new-dqp NilOperator #_FIXME))
  table-scan
  (adqc2dqp [table-scan]
    (new-dqp TableScanOperator)))

#_
(defprotocol DQPtoADQC
  (dqp2adqc [operator]))
