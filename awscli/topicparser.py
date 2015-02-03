# Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import os
import json

class TopicTagParser(object):
    VALID_TAGS = [':category:', ':description:', ':title:', ':related topic:',
                  ':service.operation']

    TOPIC_DIR = os.path.join(
        os.path.dirname(
                os.path.abspath(__file__)), 'topics')

    PRIMARY_INDEX = os.path.join(TOPIC_DIR, 'topic-tags.json')

    def __init__(self):
        self._tag_dictionary = {}

    def load_json_index(self, index_file=None):
        index_filepath = self.PRIMARY_INDEX
        if index_file is not None:
            index_filepath = index_file
        with open(index_filepath, 'r') as f:
            index = f.read()
            self._tag_dictionary = json.loads(index)

    def save_to_json_index(self, index_file=None):
        index_filepath = self.PRIMARY_INDEX
        if index_file is not None:
            index_filepath = index_file
        with open(index_filepath, 'w') as f:
            f.write(json.dumps(self._tag_dictionary, indent=4, sort_keys=True))

    def get_all_topic_names(self):
        return self._tag_dictionary.keys()

    def get_all_topic_src_files(self):
        topic_full_paths = []
        topic_names = os.listdir(self.TOPIC_DIR)
        for topic_name in topic_names:
            if not topic_name.startswith('.'):
                topic_full_path = os.path.join(self.TOPIC_DIR, topic_name)
                if topic_full_path != self.PRIMARY_INDEX:
                    topic_full_paths.append(topic_full_path)
        return topic_full_paths

    def scan(self, topic_files):
        for topic_file in topic_files:
            with open(topic_file, 'r') as f:
                topic_name = self._find_topic_name(topic_file)
                self._add_topic_name_to_dict(topic_name)
                for line in f.readlines():
                    tag, values = self._retrieve_tag_and_values(line)
                    if tag is not None:
                        self._add_tag_to_dict(topic_name, tag, values)

    def _find_topic_name(self, topic_src_file):
        # Get the name of each of these files
        topic_name_with_ext = os.path.basename(topic_src_file)
        # Strip of the .rst extension from the files
        return topic_name_with_ext[:-4]

    def _retrieve_tag_and_values(self, line):
        # This method retrieves the tag and associated value of a line. If
        # the line is not a tag, ``None`` is returned for both.

        for valid_tag in self.VALID_TAGS:
            if line.startswith(valid_tag):
                value = self._retrieve_values_from_tag(line, valid_tag)
                return valid_tag, value
        return None, None

    def _retrieve_values_from_tag(self, line, tag):
        # This method retrieves the value from a tag. Tags with multiple
        # values will be seperated by commas. All values will be returned
        # as a list.

        # First remove the tag.
        line = line.lstrip(tag)
        # Remove surrounding whitespace from value
        line = line.strip()
        # Find all values associated to the tag. Values are seperated by
        # commas.
        values = line.split(',')
        # Strip the white space from each of these values.
        for i in range(len(values)):
            values[i] = values[i].strip()
        return values

    def _add_topic_name_to_dict(self, topic_name):
        # This method adds a topic name to the dictionary if it does not
        # already exist

        # Check if the topic is in the topic tag dictionary
        if self._tag_dictionary.get(topic_name, None) is None:
            self._tag_dictionary[topic_name] = {}

    def _add_tag_to_dict(self, topic_name, tag, values):
        # This method adds a tag to the dictionary given its tag and value
        # If there are existing values associated to the tag it will add
        # only values that previously did not exist in the list.

        # Check if the topic is in the topic tag dictionary
        self._add_topic_name_to_dict(topic_name)
        # Get all of a topics tags
        topic_tags = self._tag_dictionary[topic_name]
        self._add_key_values(topic_tags, tag, values)

    def _add_key_values(self, dictionary, key, values):
        # This method adds a value to a dictionary given a key.
        # If there are existing values associated to the key it will add
        # only values that previously did not exist in the list. All values
        # in the dictionary should be lists

        if dictionary.get(key, None) is None:
            dictionary[key] = []
        for value in values:
            if value not in dictionary[key]:
                dictionary[key].append(value) 

    def query(self, tag, values=None):
        query_dict = {}
        for topic_name in self._tag_dictionary.keys():
            if self._tag_dictionary[topic_name].get(tag, None) is not None:
                tag_values = self._tag_dictionary[topic_name][tag]
                for tag_value in tag_values:
                    if values is None or tag_value in values:
                        self._add_key_values(query_dict,
                                             key=tag_value,
                                             values=[topic_name])
        return query_dict 
        

    def get_tag_value(self, topic_name, tag, default_value=None):
        if topic_name in self._tag_dictionary:
            return self._tag_dictionary[topic_name].get(tag, default_value)
        return default_value

    def _line_has_tag(self, line):
        for valid_tag in self.VALID_TAGS:
            if line.startswith(valid_tag):
                return True
        return False

    def remove_tags_from_content(self, topic_name):
        updated_lines = []
        topic_file_path = os.path.join(self.TOPIC_DIR, topic_name) + '.rst'
        with open(topic_file_path, 'r') as f:
            lines = f.readlines()
            for line in lines:
                if not self._line_has_tag(line):
                    updated_lines.append(line)
        return ''.join(updated_lines)
