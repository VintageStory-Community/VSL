namespace VSL.Domain;

public sealed class WorldRuleDefinition
{
    public required string Key { get; init; }

    public required string LabelZh { get; init; }

    public required WorldRuleType Type { get; init; }

    public IReadOnlyList<string> Choices { get; init; } = [];

    public string? DefaultValue { get; init; }

    public string? DescriptionZh { get; init; }
}
