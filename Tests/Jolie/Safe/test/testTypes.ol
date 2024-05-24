/***** Producer tests ******/
type ServiceATests{
    .throw_on_recieved*: bool
    .throw_after_update_local*: bool
    .throw_before_commit*: bool
    .throw_after_commit*: bool
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

type ProducerTests{
    .serviceA*: ServiceATests
    .outboxtests*: OutboxTestParams
    .mfsTests*: MFSTestParams
}

/***** Consumer tests ******/
type ServiceBTests{
    .throw_on_message_received*: bool
    .throw_after_local_update*: bool
}

type InboxWriterTests{
    .throw_before_updating_inbox*: bool
    .throw_after_updating_inbox_but_before_response*: bool
}

type InboxReaderTests{
    .throw_before_begin_tx*: bool
    .throw_after_update_inbox*: bool
}

type MRSTests{
    .throw_after_message_found*: bool
    .throw_after_notify_inbox_but_before_commit_to_kafka*: bool
}

type ConsumerTests {
    .serviceB*: ServiceBTests
    .inboxWriterTests*: InboxWriterTests
    .inboxReaderTests*: InboxReaderTests
    .mrsTests*: MRSTests
}

type TestExceptionType: string


interface ProducerTestInterface{
    RequestResponse:
        setupProducerTests( ProducerTests )( bool ) 
}

interface ConsumerTestInterface{
    RequestResponse:
        setupConsumerTests( ConsumerTests )( bool ) 
}
