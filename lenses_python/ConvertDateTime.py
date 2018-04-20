from datetime import datetime

class ConvertDateTime:

    def __init__(self, data, datetimelist, formatinglist):
        """

        :param data: Is all date we get of sql query
        :param datetimelist: Is a list we the name of keys which have datetime string
        :param formatinglist: Is a list of each element of datetimelist. For example
        ['%d%m%Y']. For more info about the format check this page http://strftime.org/
        :return:
        """
        self.data = data
        self.datetimelist = datetimelist
        self.formatinglist = formatinglist

    def Convert(self):
        """

        :return: self.data ,where field with datetime string convert to datetime objects
        """
        if len(self.formatinglist) == 1:
            if len(self.datetimelist) != 1:
                self.formatinglist = len(self.datetimelist) * self.formatinglist
        else:
            if len(self.formatinglist) != len(self.datetimelist):
                raise Exception("")
        convertlst = list(zip(self.datetimelist, self.formatinglist))
        for k, f in convertlst:
            for i in self.data:
                i[k] = datetime.strptime(i[k], f)
        return self.data




