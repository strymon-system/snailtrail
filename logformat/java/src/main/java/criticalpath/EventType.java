// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package criticalpath;

public enum EventType {
  START(1),
  END(2),
  SENT(3),
  RECEIVED(4);

  public int value;

  EventType(int v) {
    value = v;
  }
}

