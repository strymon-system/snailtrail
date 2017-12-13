// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package criticalpath;

import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessagePack;
import java.io.OutputStream;


/**
 * Serializes LogRecords in the common MsgPack-based format.
 */
class LogRecordPacker {

  private OutputStream _outputStream;
  private MessagePacker _packer;

  public LogRecordPacker(OutputStream outputStream) {
    _outputStream = outputStream;
    _packer = MessagePack.newDefaultPacker(_outputStream);
  }

  public void pack(
      long timestamp,
      int local_worker,
      ActivityType activity_type,
      EventType event_type,
      long correlator_id /* -1 if absent */,
      int remote_worker /* -1 if absent */,
      int operator_id) throws java.io.IOException {

      _packer.packLong(timestamp);
      _packer.packInt(local_worker);
      _packer.packInt(activity_type.value);
      _packer.packInt(event_type.value);
      _packer.packLong(correlator_id);
      _packer.packInt(remote_worker);
      _packer.packInt(operator_id);
  }

  public void close() throws java.io.IOException {
    _packer.close();
  }
}
