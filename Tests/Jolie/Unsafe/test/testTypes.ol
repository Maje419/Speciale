type ServiceATests{
    .throw_before_updating_local*: bool
    .throw_before_sending*: bool
    .throw_after_sending*: bool
}

type ServiceBTests{
    .throw_before_updating_local*: bool
    .throw_after_updating_local*: bool
    .mrs*: MRSTestParams
}

type MRSTestParams: void{
    .throw_on_message_found*: bool
    .throw_before_updating_main_service*: bool
    .throw_after_updating_main_service*: bool
    .throw_before_scheduling_timeout*: bool
}

type TestParams{
    .serviceA*: ServiceATests
    .serviceB*: ServiceBTests
}

type TestExceptionType: string
