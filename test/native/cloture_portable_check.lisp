;;;; Cloture arm of the portable-core differential oracle.
;;;;
;;;; Loads the portable core and the oracle namespaces into a Cloture image
;;;; (Clojure hosted on Common Lisp, https://github.com/ruricolist/cloture) and
;;;; runs hive-dsl.portable-check/report, whose verdict line is the arm's result.
;;;;
;;;; Usage:
;;;;   sbcl --script test/native/cloture_portable_check.lisp
;;;;
;;;; Environment:
;;;;   HIVE_DSL_ROOT  repository root (default: this file's ../..)
;;;;   CLOTURE_ROOT   a cloture checkout to put on the ASDF registry, for when
;;;;                  quicklisp cannot already find the system
;;;;
;;;; Output: one `load-fail <path> :: <condition>' line per namespace the image
;;;; could not load, then the verdict line, one of
;;;;   portable-check: PASS n/n
;;;;   portable-check: FAIL k/n           (plus one line per divergence)
;;;;   portable-check: LOAD-FAIL k/n      (the oracle namespaces are unusable)

(require :asdf)

(let ((quicklisp-init (merge-pathnames "quicklisp/setup.lisp"
                                       (user-homedir-pathname))))
  (when (and (not (find-package "QUICKLISP")) (probe-file quicklisp-init))
    (load quicklisp-init)))

(defparameter *root*
  (let ((explicit (uiop:getenv "HIVE_DSL_ROOT")))
    (if explicit
        (uiop:ensure-directory-pathname explicit)
        (uiop:pathname-parent-directory-pathname
         (uiop:pathname-parent-directory-pathname
          (uiop:pathname-directory-pathname *load-truename*)))))
  "Repository root the arm reads its sources from.")

(defparameter *core-files*
  '("src/hive_dsl/result.cljc"
    "src/hive_dsl/result/taxonomy.cljc"
    "src/hive_dsl/result/agentop.cljc"
    "src/hive_dsl/coerce.cljc"
    "src/hive_dsl/adt.cljc"
    "src/hive_dsl/adt/schema.cljc"
    "src/hive_dsl/conversation.cljc"
    "src/hive_dsl/swarm_status.cljc"
    "src/hive_dsl/resource.cljc"
    "src/hive_dsl/batch.cljc"
    "src/hive_dsl/context/identity.cljc"
    "src/hive_dsl/typed/emit.cljc")
  "The portable core, in dependency order. Cloture's `ns' does not load a
   required namespace, so the order here is load-bearing.")

(defparameter *oracle-files*
  '("test/hive_dsl/portable_cases.cljc"
    "test/hive_dsl/portable_golden.cljc"
    "test/hive_dsl/portable_check.cljc")
  "The oracle itself, in dependency order.")

(defun one-line (string)
  "STRING with every run of whitespace collapsed to one space."
  (string-trim " " (substitute #\Space #\Newline (substitute #\Space #\Tab string))))

(defun load-cljc (relative-path)
  "Load RELATIVE-PATH through Cloture's reader. Returns T, or the condition."
  (handler-case
      (progn (funcall (find-symbol "LOAD-CLOJURE" "CLOTURE")
                      (merge-pathnames relative-path *root*))
             t)
    (error (e) e)))

(defun load-all (paths)
  "Load PATHS in order, continuing past failures. Returns the failures as
   ((path . condition) ...) in load order.

   A namespace that fails partway still leaves its package and whatever it
   defined before the failure, which is what lets the per-case oracle report a
   divergence rather than nothing at all."
  (let ((failures '()))
    (dolist (path paths (nreverse failures))
      (let ((outcome (load-cljc path)))
        (unless (eq outcome t)
          (push (cons path outcome) failures))))))

(defun report-failures (failures)
  (dolist (failure failures)
    (format t "~&load-fail ~a :: ~a~%"
            (car failure) (one-line (princ-to-string (cdr failure))))))

(uiop:symbol-call '#:ql '#:quickload "cloture" :verbose nil)

(let ((cloture-root (uiop:getenv "CLOTURE_ROOT")))
  (when cloture-root
    (pushnew (uiop:ensure-directory-pathname cloture-root)
             asdf:*central-registry*
             :test #'equal)))

(defun clj-eval (source package-name)
  "Evaluate SOURCE, read as Clojure, inside PACKAGE-NAME's namespace.

   A Cloture var is not a CL special variable, so a def'd value is reachable
   only by evaluating a reference to it in its own namespace."
  (let ((package (find-package package-name)))
    (funcall (find-symbol "COMPILE-AND-EVAL" "CLOTURE")
             (funcall (find-symbol "READ-CLOJURE-FROM-STRING" "CLOTURE")
                      source :package package))))

(defun show (value)
  "VALUE printed the way Clojure would print it, on one line."
  (one-line
   (funcall (find-symbol "CALL/CLOJURE-PRINTER" "CLOTURE")
            (lambda () (prin1-to-string value)))))

(defun verdict ()
  "Run the oracle and print its verdict line, plus one line per divergence.

   Calls `diff' rather than `report': the arm must not depend on the printing
   helpers the reference implementation happens to have, only on the comparison."
  (let* ((diff (funcall (find-symbol "diff" "hive-dsl.portable-check")))
         (total (clj-eval "(count cases)" "hive-dsl.portable-cases"))
         (diverged (fset:size diff)))
    (if (zerop diverged)
        (format t "~&portable-check: PASS ~a/~a~%" total total)
        (progn
          (format t "~&portable-check: FAIL ~a/~a~%" (- total diverged) total)
          (fset:do-map (id detail diff)
            (handler-case
                (format t "  ~a expected ~a actual ~a~%"
                        (show id)
                        (show (fset:lookup detail (intern "expected" "KEYWORD")))
                        (show (fset:lookup detail (intern "actual" "KEYWORD"))))
              (error (e)
                (format t "  ~a unprintable :: ~a~%"
                        id (one-line (princ-to-string e))))))))))

(let* ((core-failures (load-all *core-files*))
       (oracle-failures (load-all *oracle-files*)))
  (report-failures (append core-failures oracle-failures))
  (if oracle-failures
      (format t "~&portable-check: LOAD-FAIL ~a/~a~%"
              (- (length *oracle-files*) (length oracle-failures))
              (length *oracle-files*))
      (verdict))
  (finish-output))
