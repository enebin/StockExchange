from abc import ABCMeta, abstractmethod


class processBase(metaclass=ABCMeta):
    def __init__(self, market, process):
        self.market = market
        self.process = process

    @abstractmethod
    def get_code_list(self):
        pass

    @abstractmethod
    def get_data(self):
        pass


class all_process(processBase):
    def __init__(self, market, process):
        super().__init__(market, process)

    def get_code_list(self):
        print(self.market)

    def get_data(self):
        print("FU")


ap = all_process(1, 2)
ap.get_code_list()