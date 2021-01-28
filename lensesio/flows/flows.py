

class LensesFlows():

    def GetFlows(self):
        __TOPOLOGY_DATA = self.GetTopology()
        __TOPOLOGY_FILTERED = {}

        for DICT_ENTRY in __TOPOLOGY_DATA[:]:
            if DICT_ENTRY['type'] in ['SOURCE', 'SINK', 'PROCESSOR']:
                __TOPOLOGY_FILTERED[DICT_ENTRY['name']] = {
                    'descendants': DICT_ENTRY['descendants'],
                    'label': DICT_ENTRY['label'],
                    'parents': DICT_ENTRY['parents'],
                    'type': DICT_ENTRY['type'],
                    'relations': {}
                }

        for __KEY in __TOPOLOGY_FILTERED.keys():
            if 'parents' in __TOPOLOGY_FILTERED[__KEY]:
                __PARENTS_LIST = __TOPOLOGY_FILTERED[__KEY]['parents']

                for __PARENT in __PARENTS_LIST:
                    for __TMP__KEY in __TOPOLOGY_FILTERED:
                        if __TMP__KEY == __KEY:
                            continue

                        if 'descendants' in __TOPOLOGY_FILTERED[__TMP__KEY]:
                            if __PARENT in __TOPOLOGY_FILTERED[__TMP__KEY]['descendants']:
                                __TOPOLOGY_FILTERED[__TMP__KEY]['relations'][__KEY] = __TOPOLOGY_FILTERED[__KEY]

        self.getFlows = __TOPOLOGY_FILTERED
        return self.getFlows
