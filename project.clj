(defproject adqc "0.0.2-SNAPSHOT"
  :description "Adaptive Distributed Query Compiler"
  ;; the dependencies also include the OGSA-DAI jars (OGSA-DAI proper
  ;; & third party); since these cannot all be obtained from a mvn
  ;; repo, I opt for manual management for now (although it might be
  ;; more reasonable to install them in the local repo)
  :licence {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"
            :distribution :repo}
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]]
  :dev-dependencies [[deview/lein-deview "1.0.5"]]
  :deview-server 9000)
