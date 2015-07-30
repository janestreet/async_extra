(* OASIS_START *)
(* OASIS_STOP *)

let dispatch = function
  | After_rules ->
    flag ["c"; "compile"] & S[A"-I"; A"src"; A"-package"; A"core"; A"-thread"];
    List.iter
      (fun tag ->
         pflag ["ocaml"; tag] "pa_ounit_lib"
           (fun s -> S[A"-ppopt"; A"-pa-ounit-lib"; A"-ppopt"; A s]))
      ["ocamldep"; "compile"; "doc"]
  | _ ->
    ()

let () = Ocamlbuild_plugin.dispatch (fun hook -> dispatch hook; dispatch_default hook)
