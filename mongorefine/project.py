from .refine import Column, RowSet, Refiner

class Project:
    """Data manipulation project"""
    def __init__(self):
        pass

    def load(self, filename):
        """Load project from .YAML file"""
        pass

    def save(self, filename):
        """Save project to .YAML file"""
        pass

    

class Filter:
    """Wrangler data filter"""
    def __init__(self):
        pass


class View:
    """Wrangler project view"""
    def __init__(self, refiner, query={}):
        self.refiner = refiner
        self.__filters = []
        self.rowset = refiner.get_rowset(query) 
        pass

    def add_filter(self, rule):
        self.__filters += rule
        pass

    def remove_filter(self, rule):
        self.__filters.remove(rule)
        pass
