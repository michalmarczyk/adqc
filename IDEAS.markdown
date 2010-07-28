# ADQC -- design ideas

## Design outline

    ; SQL query comes in
    adqc.sql
    ; query AST
    adqc.lqp
    ; initial LQP
    adqc.optimizer
    ; optimised LQP
    adqc.runtime.ogsa-dai
    ; sth digestible to the runtime

The basic optimisation heuristic could be as follows:

1. Split all relations involved in the query into groups based on whether
   * they reside on a single physical resource;
   * they are linked through the transitive closure of the relation
     '... participates in a non-full-cross-product join with ...'

2. Push the join of each such group down to the physical data resource; if cross
   products need to be formed, have our runtime handle that towards the end of
   the processing of the query.

## Utility functions

    (defn unary? [pred] (== 1 (arity pred)))
    ;; if the concept of "arity" is useful elsewhere,
    ;; could make this a proto/multimethod
    (defn arity [pred] (-> pred attributes count))

    ;; a 'sargable' predicate here is one which can be processed
    ;; entirely inside a single physical DB
    (defn sargable? [pred]
      (->> pred
           attributes
           (mapcat :src) ; actually I need to extract the physical resource
           (into #{})
           count
           (== 1)))

`sargable?` could useful in `GROUP BY` processing etc.

## Zipping LQPs

1. Consider providing something like `clojure.core/empty` (in the form of
   a protocol) to help when doing tree rotations.

2. Zippers might be useful for implementing doubly-linked trees for consumption
   from Java if the actual uk.org.ogsadai...Operators etc. are objects wrapping
   a zipper on an underlying Clojure tree-like structure. Most operations would
   just delegate to the current node, while .getParent would take advantage of
   the zipper op.

## Misc. ideas / open questions

1. How about asking the source for *ordered* output *always*?

2. How to go about normalising complex predicates (which might use arbitrary
   logical connectives)?

3. Which things need to be `java.io.Serializable` (for handing off to
   the runtime)?

4. An opportunity for adaptive behaviour: as long as there is hope that the
   outer relation in a join is small, try using nested loops, but switch to
   merge scan once it becomes clear that would be the superior approach.

5. Do I need a rename operator or can I leave it out, but hide implicit renames
   in all other ops?
