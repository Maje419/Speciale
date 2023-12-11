type TestParams{
    .serviceA* {
        .throw_before_updating_local*: bool
        .throw_before_sending*: bool
        .throw_after_sending*: bool
    }
}

type TestExceptionType: string
