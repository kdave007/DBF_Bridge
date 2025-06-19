


class OP:
    def execute(self, operations):
        if "create" in operations:
            self._create(operations['create'])

        if "update" in operations:
           self._update(operations['update'])

        if "delete" in operations:
            self._delete(operations['delete'])     



    def _create(self, records):
        for record in records:
            print(f'RECORD FOUND {record}')
            print(f'------')

    def _update(self, records):
        for record in records:
            print(f'RECORD FOUND {record}')
            print(f'------')

    def _delete(self, records):
        for record in records:
            print(f'RECORD FOUND {record}')
            print(f'------')

    


