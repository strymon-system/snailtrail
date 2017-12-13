// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package criticalpath;

public enum ActivityType {
  INPUT             (1),
  BUFFER            (2),
  SCHEDULING        (3),
  PROCESSING        (4),
  BARRIERPROCESSING (5),
  SERIALIZATION     (6),
  DESERIALIZATION   (7),
  FAULTTOLERANCE    (8),
  CONTROLMESSAGE    (9),
  DATAMESSAGE       (10);

  public int value;

  ActivityType(int v) {
    value = v;
  }
}

