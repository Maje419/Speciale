from ..test.testTypes import TestParams, TestExceptionType

interface ServiceBInterface{
    RequestResponse: 
        setupTest( TestParams )( bool )
    OneWay: 
        updateLocalDb( string )
}