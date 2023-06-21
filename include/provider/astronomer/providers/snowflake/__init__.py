
"""Description of the package"""

__version__ = "0.0.1-dev0"

def get_provider_info():
    return {
        "package-name": "astro-provider-snowflake", 
        "name": "Snowpark Airflow Provider", 
        "description": "Decorators for Snowpark.", 
        "hook-class-names": [

        ],
        "extra-links": [
            ".decorators.snowpark.snowpark_python_task",
            ".decorators.snowpark.snowpark_virtualenv_task",
            ".decorators.snowpark.snowpark_ext_python_task"
        ],
        "versions": ["0.0.1-dev0"],
    }