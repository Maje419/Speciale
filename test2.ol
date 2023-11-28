from console import Console

interface TestI {
}

service Test {
    execution:single
    
    embed Console as Console

    main
    {
        x = 10 | x = "Hello"

        println@Console()()
    }
}
