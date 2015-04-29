module Debug (Bus : module type of Bus) : sig

  include module type of struct include Bus end

  val check_invariant : bool ref
  val show_messages   : bool ref

end
