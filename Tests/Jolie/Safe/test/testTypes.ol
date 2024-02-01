/***** Consumer tests ******/
type ServiceATests{
    .throw_before_outbox_call*: bool
    .throw_after_outbox_call*: bool
}

type OutboxTestParams {
    .throw_before_insert*: bool
    .throw_after_insert*: bool
}

type MFSTestParams {
    .throw_before_check_for_messages*: bool
    .throw_after_message_found*: bool
    .throw_after_send_but_before_delete*: bool
}

type ConsumerTests{
    .serviceA*: ServiceATests
    .outboxtests*: OutboxTestParams
    .mfsTests*: MFSTestParams
}

/***** Producer tests ******/
type SeriveBTests{
    .throw_before_search_inbox*: bool
    .throw_at_inbox_message_found*: bool
    .throw_after_transaction*: bool
}

type ProducerTests {
    .serviceB*: ServiceBTests
    .inboxTests*: InboxTestParams
    // .mrsTests*: MRSTestParams
}

type TestExceptionType: string
