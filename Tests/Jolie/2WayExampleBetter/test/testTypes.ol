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

type TestRequest {
    .serviceA: ServiceATestParam
    .outboxService: OutboxServiceTestParams
    .MFS: MFSTestParams
}

interface TestInterface {
    RequestResponse: setupTest( TestRequest )( bool )
}