// Test CSI gRPC services of mayastor.
//
// It used to be possible to start this test suite against external mayastor
// instance to verify it. But later we dropped this feature because stage and
// publish volume tests became really unsuitable for this type of operation.
// We could split the test suite in future if we want this functionality at
// least for some tests where it is possible to do.
//
// It is a mess to work with nbd devices. If nbd device is attached to kernel
// then detached and immediately attached again we see all kinds of issues.
// That's why we use a different nbd device for each stage operation so that
// we don't confuse the kernel :-(

'use strict';

const csiCommon = require('./test_csi_common');
const enums = require('./grpc_enums');

csiCommon.csiProtocolTest('NBD', enums.NEXUS_NBD, 10000, { uri: 'file:///dev/nbd' + '9' });
