import pytest

from dataengineeringutils3.pulumi import Tagger


@pytest.mark.parametrize(
    "input,output", [("alpha", True), ("prod", True), ("dev", False)]
)
def test_is_production(input, output):
    tagger = Tagger(environment_name=input)
    print(tagger._global_tags)
    tags = tagger.create_tags("test")
    assert tags["is-production"] == output


def test_invalid_kwarg():
    with pytest.raises(KeyError):
        Tagger(environment_name="prod", is_production=True)
    tagger = Tagger(environment_name="prod")
    with pytest.raises(KeyError):
        tagger.create_tags("test", is_production=True)


@pytest.mark.parametrize(
    "input", ["HQ", "HMPPS", "OPG", "LAA", "HMCTS", "CICA", "Platforms"]
)
def test_valid_business_unit(input):
    tagger = Tagger(environment_name="prod", business_unit=input)
    tagger.create_tags("test", business_unit=input)


def test_invalid_business_unit():
    with pytest.raises(ValueError):
        Tagger(environment_name="prod", business_unit="DASHRAS")
    tagger = Tagger(environment_name="prod")
    with pytest.raises(ValueError):
        tagger.create_tags("test", business_unit="DASHRAS")


def test_kwarg_passthrough():
    tagger = Tagger(environment_name="prod", extra_tag="test")
    tags = tagger.create_tags("test")
    assert tags["extra-tag"] == "test"


def test_kwarg_tag_names():
    tagger = Tagger(environment_name="prod", Extra_Global_Tag="test")
    tags = tagger.create_tags("test", Extra_Local_Tag="test")
    assert tags["extra-global-tag"] == "test"
    assert tags["extra-local-tag"] == "test"


def test_all_mandatory_tags_exist():
    tagger = Tagger(environment_name="prod")
    tags = tagger.create_tags("test")
    for mandatory_tag in [
        "Name",
        "business-unit",
        "application",
        "owner",
        "is-production",
    ]:
        assert mandatory_tag in tags.keys()


def test_create_tags_overwrite():
    tagger = Tagger(environment_name="prod")
    tags = tagger.create_tags("test")
    assert tags["business-unit"] == "Platforms"
    tags = tagger.create_tags("test", business_unit="HQ")
    assert tags["business-unit"] == "HQ"
