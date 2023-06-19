
"""Description of the package"""

__version__ = "0.0.1-dev0"

def get_provider_info():
    return {
        "package-name": "astro-provider-snowflake", 
        "name": "SnowparkContainers and Snowpark Airflow Provider", 
        "description": "Decorator providers for SnowparkContainers and Snowpark.", 
        "hook-class-names": [
            "include.astronomer.providers.snowflake.hooks.snowpark_containers.SnowparkContainersHook",
            ],
        "extra-links": [
            "include.astronomer.providers.snowflake.decorators.snowpark_containers.snowpark_containers_python",
            "include.astronomer.providers.snowflake.decorators.snowpark.dataframe_decorator"
        ],
        "versions": ["0.0.1-dev0"],
    }