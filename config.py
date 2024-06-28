import yaml
class Config:
    def __init__(self, config_path):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
    @property
    def input_paths(self):
        return self.config['input_paths']
    @property
    def output_path(self):
        return self.config['output_path']
    @property
    def columns(self):
        return self.config['columns']
    @property
    def aggregations(self):
        return self.config['aggregations']