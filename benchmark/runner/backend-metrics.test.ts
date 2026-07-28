import { describe, expect, it } from 'vitest';

import { deriveBackendMetrics } from './backend-metrics.js';

describe('deriveBackendMetrics', () => {
  it('separates heartbeat, replication, vote, and snapshot traffic', () => {
    const derived = deriveBackendMetrics({
      raft_grpc_append_heartbeat_request_bytes: 400,
      raft_grpc_append_heartbeat_requests: 4,
      raft_grpc_append_replication_entries: 6,
      raft_grpc_append_replication_request_bytes: 1000,
      raft_grpc_append_replication_requests: 2,
      raft_grpc_append_response_bytes: 180,
      raft_grpc_snapshot_payload_bytes: 1600,
      raft_grpc_snapshot_request_bytes: 2000,
      raft_grpc_snapshot_requests: 2,
      raft_grpc_snapshot_response_bytes: 40,
      raft_grpc_vote_request_bytes: 50,
      raft_grpc_vote_requests: 2,
      raft_grpc_vote_response_bytes: 30,
    });

    expect(derived).toMatchObject({
      raftGrpcAppendHeartbeatRequestBytesPerRequest: 100,
      raftGrpcAppendReplicationEntriesPerRequest: 3,
      raftGrpcAppendReplicationRequestBytesPerRequest: 500,
      raftGrpcAppendResponseBytesPerRequest: 30,
      raftGrpcSnapshotPayloadBytesPerRequest: 800,
      raftGrpcSnapshotRequestBytesPerRequest: 1000,
      raftGrpcSnapshotResponseBytesPerRequest: 20,
      raftGrpcVoteRequestBytesPerRequest: 25,
      raftGrpcVoteResponseBytesPerRequest: 15,
    });
  });
});
