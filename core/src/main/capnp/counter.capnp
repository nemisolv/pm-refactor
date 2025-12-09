@0xcafebabef00d1234;

using Java = import "java.capnp";
$Java.package("com.viettel");
$Java.outerClassname("CounterSchema");

struct CounterValue {
  id @0    :UInt32;
  value @1 :Int64;
}

struct CounterData {
  time @0       :Int64;
  duration @1   :Int32;
  location @2   :Text;
  cell @3       :Int64;
  service @4    :Int64;
  tac @5        :Int64;
  code @6       :Int64;
  condition @7  :Int64;
  data @8       :List(CounterValue);
}

struct CounterDataCollection {
  data @0 :List(CounterData);
  unit @1 :Text;
  type @2 :Int64;
}