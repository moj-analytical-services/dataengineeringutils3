class Tagger:
    def __init__(
        self,
        environment_name: str,
        business_unit: str = "Platforms",
        allowed_business_units: list = [
            "HQ",
            "HMPPS",
            "OPG",
            "LAA",
            "HMCTS",
            "CICA",
            "Platforms",
        ],
        application: str = "Data Engineering",
        owner: str = "Data Engineering:dataengineering@digital.justice.gov.uk",
        **kwargs,
    ):
        """
        Provides a Tagger resource.

        Parameters
        ----------
        environment_name : str
            The name of the environment in which resources are deployed, for example,
            "alpha", "prod" or "dev".
        business_unit : str, optional
            The business unit of the team that owns the resources. Should be one of
            allowed_business_units.
            By default "Platforms".
        allowed_business_units : list, optional
            A list of allowed business units.
            By default, ["HQ", "HMPPS", "OPG", "LAA", "HMCTS", "CICA", "Platforms"].
        application : str, optional
            The application in which the resources are used.
            By default "Data Engineering".
        owner : str, optional
            The owner of the resources. This should be of the form
            <team-name>:<team-email>.
            By default "Data Engineering:dataengineering@digital.justice.gov.uk".

        """
        self._allowed_business_units = allowed_business_units
        self._check_business_unit(business_unit, self._allowed_business_units)
        if "is_production" in kwargs:
            raise KeyError("is_production is not an allowed argument")

        self._global_tags = {
            "environment_name": environment_name,
            "business_unit": business_unit,
            "application": application,
            "owner": owner,
        }
        self._global_tags.update(kwargs)

    def _check_business_unit(self, business_unit: str, allowed_business_units: list):
        """Checks if business_unit is an allowed value

        Parameters
        ----------
        business_unit : str
            The business unit of the team that owns the resources. This should be one of
            allowed_business_units.
        allowed_business_units : list
            A list of allowed business units.

        Raises
        ------
        ValueError
            If the value of business_unit is not allowed a ValueError will be raised.
        """
        if business_unit not in allowed_business_units:
            raise ValueError(
                f"business_unit must be one of {', '.join(allowed_business_units)}"
            )

    def create_tags(self, resource_name: str, **kwargs) -> dict:
        """
        Creates a dictionary of mandatory and custom tags that can be passed to the tags
        argument of a Pulumi resource.

        Parameters
        ----------
        resource_name : str
            The name of the resource for which the tags will be created. This should be
            the same as the resource_name of the Pulumi resource to which you are
            adding the tags.

        Returns
        -------
        dict
            A dictionary of mandatory and custom tags that can be passed to the tags
            argument of a Pulumi resource.
        """
        init_tags = self._global_tags
        init_tags.update(kwargs)
        tags = {}
        for key, value in init_tags.items():
            if key == "business_unit":
                self._check_business_unit(value, self._allowed_business_units)
            if key in ["is_production"]:
                raise KeyError(f"{key} is not an allowed argument")
            tags[key.replace("_", "-").lower()] = value
        tags["is-production"] = tags["environment-name"] in ["alpha", "prod"]
        tags["Name"] = resource_name
        return tags
