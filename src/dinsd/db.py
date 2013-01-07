#Copyright 2012 R. David Murray (see end comment for terms).
"""Common code used by the various XXX_db modules"""


class ConstraintError(Exception):
    pass


class RowConstraintError(ConstraintError):

    def __init__(self, relname, cname, constraint, invalid):
        self.relname = relname
        self.cname = cname
        self.constraint = constraint
        self.invalid = invalid

    def __str__(self):
        return ("{} constraint {} violated: {!r} is not satisfied by "
                "{!r}").format(self.relname,
                               self.cname,
                               self.constraint,
                               self.invalid)


class Rollback(Exception):
    pass


class DBConstraintLoop(ConstraintError):

    def __str__(self):
        return "Database constrain-and-update loop did not terminate"



#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.
