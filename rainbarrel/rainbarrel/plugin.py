# ________________________________________________________________________
#
#  Copyright (C) 2015 Andrew Fullford
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# ________________________________________________________________________
#
 
class PluginError(Exception):
	"""
	This exception is used to distinguish errors detected internally
	by a plugin from others that might occur during plugin operation.
"""
	pass

class Plugin(object):
	def __init__(self):
		pass

	def handler(self, parent, **params):
		"""
		This method is called when any POST is received from the
		device.  The args are:

			parent	The Barrel instance that received the POST.

			params	Arbitrary params yet to be determined.


		Generally, the most useful attribute of the parent is parent.state
		which is a dict of the currently recorded state.  This includes a
		"_last_element" element which indicates the subject area of the POST
		that triggered the change.

		Plugins can make use of methods in the parent.  In particular,
		plugins can log via the logging instance, "parent.log".

		Note that a Barrel object will instantiate all classes in
		a plugin module that are subclasses of this class.
	"""
		raise PluginError("The handler method has not been overridden from the base class")
