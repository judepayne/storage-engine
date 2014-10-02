# TODOs

## General


## Core.clj
### Phase 1
0. **DONE** get-current & get-snap FNS
1. **DONE** ADD ERROR HANDLING - check alive?
2. **DONE** USE SNAPSHOT FOR SNAPSHOT CREATION RATHER THAN CURRENT STATE (DB UPDATED IN RUNNING = BAD)
3. *NOT-DOING* THINK ABT JAVA INTERFACE - ITERATOR RATHER THAN CHANNEL
      Plan for the Java interface is as follows:
      - Make a Protocol called 'StorageEngine' with implments the various Clojure api functions
      - For `methods` which natively return lazy clojure seqs (e.g. get-current & get-snap),
        create a function which converts a lazy seq into a java Iterator. Will probably need another
        protocol to represent this iterator.
4. **DONE**BETTER SCHEME FOR CONFIG - PASS MAP TO STARTUP THEN DEFAULT TO VALUES IN THE LOCAL FILE?
5. **NOT-DOING** RENAME STATE CHANGING FUNCTIONS USING BANG !
6. **DONE** THINK ABT WHETHER SNAPS SHOULD BE OPN AT START: HMM NO
7. TEST SNAPPING WITH LARGE KEYSETS


### Phase 2
8. TRANSACTION MODE & UPDATE FN
9. IMMUTABLE MODE - composite Key-timestamp key
10. REMOTE INDEX - Tied to Immutable mode?