namespace ClockworkDb.Engine.Tests;

public abstract class TestBase
{
	private protected readonly record struct SimpleTestEntity
		(long Id, string Info);
}