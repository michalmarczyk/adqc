(use '[clojure.contrib.repl-utils :only [show]]
     'clojure.pprint)

(require '[clojure
           [zip :as zip]
           [walk :as walk]
           [set :as set]
           [test :as test]
           [string :as str]]
         '[clojure.contrib [string :as cstr] [io :as io]])

(defmacro undef [& syms]
  `(doseq [s# '~syms] (ns-unmap *ns* s#)))
