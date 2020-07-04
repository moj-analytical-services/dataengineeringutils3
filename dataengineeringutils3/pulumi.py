def check_business_unit(business_unit: str):
    """Checks if business_unit is valid.

    Parameters
    ----------
    business_unit : str
        The business unit of the team that owns the resources. This should be one of
        "HQ", "HMPPS", "OPG", "LAA", "HMCTS", "CICA", "Platforms".

    Raises
    ------
    ValueError
        If the value of business_unit is valid a ValueError will be raised.
    """
    if business_unit not in [
        "HQ",
        "HMPPS",
        "OPG",
        "LAA",
        "HMCTS",
        "CICA",
        "Platforms",
    ]:
        raise ValueError(
            "business_unit must be one of HQ, HMPPS, OPG, LAA, HMCTS, CICA, or"
            "Platforms"
        )


class Tagger:
    def __init__(
        self,
        environment_name: str,
        business_unit: str = "Platforms",
        application: str = "Data Engineering",
        owner: str = "Data Engineering:dataengineering@digital.justice.gov.uk",
        **kwargs
    ):
        """
        Provides a Tagger resource.

        Parameters
        ----------
        environment_name : str
            The name of the environment in which resources are deployed, for example,
            "alpha", "prod" or "dev".
        business_unit : str, optional
            The business unit of the team that owns the resources. This should be one of
            "HQ", "HMPPS", "OPG", "LAA", "HMCTS", "CICA", "Platforms".
            By default "Platforms".
        application : str, optional
            The application in which the resources are used.
            By default "Data Engineering".
        owner : str, optional
            The owner of the resources. This should be of the form
            <team-name>:<team-email>.
            By default "Data Engineering:dataengineering@digital.justice.gov.uk".

        """
        check_business_unit(business_unit)
        if "is_production" in kwargs:
            raise KeyError("is_production is not an allowed argument")

        self._global_tags = {
            "environment_name": environment_name,
            "business_unit": business_unit,
            "application": application,
            "owner": owner,
        }
        self._global_tags.update(kwargs)

    def create_tags(self, resource_name: str, **kwargs) -> dict:
        """
        Creates a dictionary of mandatory and custom tags that can be passed to the tags
        argument of a Pulumi resource.

        Parameters
        ----------
        resource_name : str
            The name of the resource for which the tags will be created.

        Returns
        -------
        dict
            A dictionary of mandatory and custom tags that can be passed to he tags
            argument of a Pulumi resource.
        """
        init_tags = self._global_tags
        init_tags.update(kwargs)
        tags = {}
        for key, value in init_tags.items():
            if key == "business_unit":
                check_business_unit(value)
            if key == "is_production":
                raise KeyError("is_production is not an allowed argument")
            tags[key.replace("_", "-").lower()] = value
        tags["is-production"] = tags["environment-name"] in ["alpha", "prod"]
        tags["Name"] = resource_name
        return tags
