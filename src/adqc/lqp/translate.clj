(ns adqc.lqp.translate
  (:use adqc.lqp.operators))

(defprotocol ADQCtoDQP
  (adqc2dqp [operator]))



#_
(defprotocol DQPtoADQC
  (dqp2adqc [operator]))
