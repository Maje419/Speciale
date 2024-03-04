type ServiceATestParam{
    .throw_on_updateNumber_called: bool
    .throw_after_local_updates_executed: bool
    .throw_on_finalizeChoreography_called: bool
    .throw_after_transaction_committed: bool
}

type OutboxServiceTestParams{
    .throw_on_updateOutbox_called: bool
    .throw_after_transaction_committed: bool
}

type MFSTestParams{
    .throw_after_message_found: bool
    .throw_before_commit_to_kafka: bool
}

type InboxWriterTestParams {
    .recieve_called_throw_before_update_local: bool
}

type MRSTestParams{
    .throw_after_message_found: bool
    .throw_after_notify_inbox_but_before_commit_to_kafka: bool
}

type InboxServiceTestParams{
    .throw_before_insert: bool
    .throw_after_insert: bool
}

type TestRequest {
    .serviceA: ServiceATestParam
    .outboxService: OutboxServiceTestParams
    .MFS: MFSTestParams
    .inboxService: InboxServiceTestParams
    .inboxWriterTests: InboxWriterTestParams
    .MRS: MRSTestParams
}

interface TestInterface {
    RequestResponse: setupTest( TestRequest )( bool )
}