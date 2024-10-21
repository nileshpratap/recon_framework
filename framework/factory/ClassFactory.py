import importlib
import pkgutil
import inspect
import logging as logs
import framework.engines


def list_submodules(list_name, package_name):
    for loader, module_name, is_pkg in pkgutil.walk_packages(package_name.__path__, package_name.__name__ + '.'):
        try:
            list_name.append(module_name)
            module_name = __import__(module_name, fromlist='dummylist')
            if is_pkg:
                list_submodules(list_name, module_name)
        except ModuleNotFoundError as e:
            logs.warning(e)


class ClassFactory(object):
    @staticmethod
    def getEngineClass(Engine, Config=None):
        if (Engine.strip() == ''):
            raise ValueError("::::Engine Name can not be Empty/Null.")

        all_modules = []
        list_submodules(list_name=all_modules, package_name=framework.engines)

        try:
            for module_class in all_modules:
                Path = str(module_class)
                module = importlib.import_module(Path)
                for name, attrq in inspect.getmembers(module, predicate=inspect.isclass):
                    class_Name = getattr(module, name)
                    if hasattr(class_Name, "Engine"):
                        if (class_Name(Config).Engine.upper() == Engine.upper()):
                            return class_Name(Config)
            raise ValueError("::::Engine with name {} not found".format(Engine))
        except (ImportError, AttributeError) as e:
            raise ImportError('::::Class Not Found for the Engine Name Passed {}'.format(Engine))

