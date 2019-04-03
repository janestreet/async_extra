open Qtest_lib.Std

let tests = Udp_test.tests
let () = Runner.main tests
