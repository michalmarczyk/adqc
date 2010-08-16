(ns adqc.sql
  (:use [adqc.sql protocols records]
        [clojure
         [zip :as zip :only []]
         [walk :as walk :only []]
         [set :as set :only []]
         [string :as str :only []]]
        [clojure.contrib
         #_[string :as cstr :only []]
         [def :only [defalias]]])
  (:import uk.org.ogsadai.parser.sql92query.SQLQueryParser
           org.antlr.runtime.tree.CommonTree
           org.antlr.runtime.CommonToken))

(defmacro defantlrparsers [& m&fs]
  `(let ~'[sql-query-parser (SQLQueryParser/getInstance)]
     ~@(for [[method-suffix fn-suffix]
             (into {} (map vec (partition 2 m&fs)))]
         `(defn ~(symbol (str "antlr-parse-" fn-suffix))
            ~(str "Parses the given SQL string with the "
                  "parseSQL" method-suffix
                  " method of the ANTLR-based praser.")
            [~'sql]
            (. ~'sql-query-parser
               ~(symbol (str "parseSQL" method-suffix))
               ~'sql)))))

(defantlrparsers
  ""                       sql
  ForCondition             condition
  ForDerivedColumn         derived-column
  ForLiteral               literal
  ForValueExpressionOrStar value-expression-or-star)

(defalias antlr-parse-expression antlr-parse-value-expression-or-star)

(defn antlr-ast-zip
  [root]
  (zip/zipper (constantly true)
              (fn children [node]
                (let [cnt (.getChildCount ^CommonTree node)]
                  (map #(.getChild ^CommonTree node %) (range cnt))))
              ;; this is wrong, make-node should return
              ;; a *new* branch node
              #_
              (fn make-node [node ^java.util.Collection children]
                (.addChildren ^CommonTree node
                              (java.util.ArrayList. children)))
              (fn make-node [node children]
                (throw
                 (Exception.
                  "adding nodes to ANTLR AST zipper not supported")))
              root))

;;; this strikes me as less then satisfactory
(def node-type-numbers
     {4  ::antlr-statement
      5  ::antlr-query
      8  ::antlr-select-list
      9  ::antlr-from-list
      10 ::antlr-where
      13 ::antlr-relation
      14 ::antlr-column
      15 ::antlr-function
      16 ::antlr-not
      18 ::antlr-tablecolumn
      23 ::antlr-is-null
      30 ::antlr-id
      31 ::antlr-int
      34 ::antlr-string
      49 ::antlr-star
      65 ::antlr-infix-plus
      66 ::antlr-infix-minus
      67 ::antlr-infix-div
      79 ::antlr-or
      80 ::antlr-and
      84 ::antlr-between
      86 ::antlr-=
      87 ::antlr-<>
      88 ::antlr-!=
      89 ::antlr-<
      90 ::antlr->
      91 ::antlr->=
      92 ::antlr-<=
      95 ::antlr-like})

;;; do I want to haul some context around
;;; (to be queried for the source of an attribute etc.)?
;;; NB. a context argument could be used to disambiguate stars
(defmulti transform-node (comp node-type-numbers :ntype))

(defmacro deftn [dispatch-val children-pat & body]
  `(defmethod transform-node ~dispatch-val
     [{~children-pat :children}]
     ~@body))

(defmethod transform-node nil [token] token)

(deftn ::antlr-function [f & args]
  (make-function-application-expression f args))

(deftn ::antlr-tablecolumn children
  (if (next children)
    (make-attribute (second children) (first children) nil)
    (make-attribute (first children) nil nil)))

(defmacro deftion [dispatch-val op]
  `(deftn ~dispatch-val [lhs# rhs#]
     (make-infix-operator-expression
      (make-infix-operator ~(str op)) lhs# rhs#)))

(deftion ::antlr-infix-plus +)
(deftion ::antlr-infix-minus -)
(deftion ::antlr-infix-div /)

(defmacro deftipn [dispatch-val pred]
  `(deftn ~dispatch-val [lhs# rhs#]
     (make-infix-predicate-expression
      (make-infix-predicate ~(str pred)) lhs# rhs#)))

(deftipn ::antlr-= =)
(deftipn ::antlr-< <)
(deftipn ::antlr-> >)
(deftipn ::antlr-<= <=)
(deftipn ::antlr->= >=)
(deftipn ::antlr-!= !=)
(deftipn ::antlr-<> <>)
(deftipn ::antlr-like LIKE)

(deftn ::antlr-is-null [col] (make-is-null-expression col))

(deftn ::antlr-and [lhs rhs] (make-and-expression lhs rhs))
(deftn ::antlr-or  [lhs rhs] (make-or-expression lhs rhs))
(deftn ::antlr-not [pred]    (make-negation-expression pred))

;;; do I want to have a separate Between record instead?
(deftn ::antlr-between [attr low high]
  (let [leq (make-infix-predicate "<=")]
    (make-and-expression
     (make-infix-predicate-expression leq low attr)
     (make-infix-predicate-expression leq attr high))))

(defmethod transform-node ::antlr-id [{text :text}] text) ; is this ok?
(defmethod transform-node ::antlr-int [{text :text}] (BigInteger. text))
(defmethod transform-node ::antlr-string [{text :text}] text)

(deftn ::antlr-star children
  (if-let [[x y] (seq children)]
    (make-infix-operator-expression (make-infix-operator "*") x y)
    (make-column-star)))

(deftn ::antlr-column [attr & [as]]
  (make-column-expression attr as))

(deftn ::antlr-select-list [& cols] (make-select-list cols))
(deftn ::antlr-from-list [& sources] (make-from-list sources))
(deftn ::antlr-relation [id & [as]] (make-relation-expression id as))
(deftn ::antlr-where [pred] (make-where-clause pred))
;;; TODO: add fields for GROUP BY & HAVING
(deftn ::antlr-query [select-list from-list where-clause]
  (make-sql-query select-list from-list where-clause))
;;; TODO: add field for ORDER BY
(deftn ::antlr-statement [query] (make-sql-statement query))

;;; Clojure maps and vectors will only occur here where antlr->clojure
;;; constructs them, so I'm free to use map? and vector? to determine
;;; whether I'm changing something in postwalk.
(defn transform-ast [ast]
  (walk/postwalk
   (fn [item]
     (cond (map? item) (transform-node item)
           ;; vec necessary (because clojure.lang.MapEntry satisfies vector?)
           ;; -- I wonder if there is potential for breakage that I can't see
           (vector? item) (vec (map transform-node item))
           :else item))
   ast))

(defmacro defparsefns [& s&ts]
  `(do ~@(for [[suffix text-type] (into {} (map vec (partition 2 s&ts)))
               :let [fn-name (str "parse-" suffix)]]
           `(defn ~(symbol fn-name)
              ~(str "Parses the given " text-type " with OGSA-DAI's ANTLR-based\n"
                    "parser and converts the result to a Clojure tree.")
              [~'sql]
              (-> ~'sql
                  ~(symbol (str "antlr-" fn-name))
                  antlr->clojure
                  transform-ast)))))

(defparsefns
  sql            "SQL"
  expression     "SQL expression"
  condition      "SQL predicate"
  literal        "SQL literal"
  derived-column "SQL derived column")
