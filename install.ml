#use "topfind";;
#require "js-build-tools.oasis2opam_install";;

open Oasis2opam_install;;

generate ~package:"async_extra"
  [ oasis_lib "async_extra"
  ; file "META" ~section:"lib"
  ]
