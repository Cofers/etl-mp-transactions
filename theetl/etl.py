import yaml
import importlib
import logging

# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETL:
    """
    A class to manage the ETL (Extract, Transform, Load) process based on configuration specified in a YAML file.
    
    Attributes:
        loads (list): A list of loading functions.
        load_names (list): A list of names for the loading functions.
        transformations (list): A list of transformation functions.
        transformation_names (list): A list of names for the transformation functions.
        filters (list): A list of filter functions.
        filter_names (list): A list of names for the filter functions.
        extraction (function): The extraction function.
        extraction_name (str): The name of the extraction function.
    """

    def __init__(self, config_path, config_name):
        """
        Initializes the ETL process by loading configurations from a YAML file based on the provided name.
        
        Parameters:
            config_path (str): The file path to the YAML configuration file.
            config_name (str): The specific configuration name to load.
        """
        configs = self.read_yaml(config_path)
        if configs:
            config = next((item for item in configs if item.get('name') == config_name), None)
            if config:
                self.loads, self.load_names = self.load_module_functions(config.get('loads', []))
                self.transformations, self.transformation_names = self.load_module_functions(config.get('transformations', []))
                self.filters, self.filter_names = self.load_module_functions(config.get('filters', []))
                self.extraction, self.extraction_name = self.load_module_function(config.get('extraction'))
                logging.info(f"ETL configuration loaded: {config_name}")
            else:
                logger.error(f"No configuration found with the name: {config_name}")
        else:
            logger.error("No configurations loaded from the file.")

    @staticmethod
    def read_yaml(path):
        """
        Reads a YAML file and returns its content.

        Parameters:
            path (str): The path to the YAML file.

        Returns:
            list: The content of the YAML file.
        """
        try:
            with open(path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            logger.error(f"The file was not found at path: {path}")
            return []

    def load_module_function(self, module_function_str):
        """
        Dynamically loads a function from a specified module.

        Parameters:
            module_function_str (str): A string in the format 'module.function_name'.

        Returns:
            tuple: A tuple containing the function and its name, or (None, None) if an error occurs.
        """
        if module_function_str:
            try:
                module_name, function_name = module_function_str.rsplit('.', 1)
                module = importlib.import_module(module_name)
                func = getattr(module, function_name)
                return func, function_name
            except ImportError as e:
                logger.error(f"Error importing module: {e}")
                return None, None
            except AttributeError as e:
                logger.error(f"Error accessing attribute: {e}")
                return None, None
        return None, None

    def load_module_functions(self, module_functions_list):
        """
        Loads multiple module functions from a list of strings.

        Parameters:
            module_functions_list (list of str): List of strings specifying the functions to load.

        Returns:
            tuple: Two lists containing the functions and their names respectively.
        """
        functions = []
        function_names = []
        for func_str in module_functions_list:
            func, name = self.load_module_function(func_str)
            if func:
                functions.append(func)
                function_names.append(name)
        return functions, function_names

    def get_function_names(self, function_type):
        """
        Returns the names of the functions of a specific type.

        Parameters:
            function_type (str): The type of functions (e.g., 'loads', 'transformations').

        Returns:
            list: A list of function names.
        """
        return getattr(self, f"{function_type}_names", [])

    def run_extraction(self, data):
        """
        Runs the extraction function if configured.

        Parameters:
            data: The data to process.

        Returns:
            The result of the extraction function or None.
        """
        if self.extraction:
            return self.extraction(data)
        logger.error("Extraction function not configured.")

    def run_transformations(self, data):
        """
        Sequentially applies each transformation function to the data.

        Parameters:
            data: The data to transform.

        Returns:
            The transformed data.
        """
        for transformation in self.transformations:
            data = transformation(data)
        return data

    def run_filters(self, data):
        """
        Sequentially applies each filter function to the data.

        Parameters:
            data: The data to filter.

        Returns:
            The filtered data.
        """
        for filter_func in self.filters:
            data = filter_func(data)
        return data

    def run_loads(self, data):
        """
        Runs all configured load functions on the data.

        Parameters:
            data: The data to load.
        """
        for load in self.loads:
            load(data)

    def run_etl(self, data):
        """
        Executes the full ETL process: extraction, transformations, filters, and loads.

        Parameters:
            data: The initial data to process through the ETL pipeline.

        Returns:
            None
        """
        data = self.run_extraction(data)
        data = self.run_transformations(data)
        data = self.run_filters(data)
        self.run_loads(data)
