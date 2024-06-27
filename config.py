import yaml

class Config:
    def __init__(self, config_path):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
    @property
    def input_paths(self):
        return self.config['input_paths']
