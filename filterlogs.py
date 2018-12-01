#!/usr/bin/env python3.6

from argparse import ArgumentParser
from multiprocessing import Pool
from os import listdir
from os.path import isfile, join
from re import search
from sys import stderr


def parse_configuration_file(configuration_file_name):
    """
    Parses the configuration file
    :param configuration_file_name: Name of the configuration file
    :return: Configuration dictionary
    """
    configuration_dict = {}
    with open(configuration_file_name) as file:
        for line in file:
            stripped_line = line.rstrip('\r\n')
            search_object = search('^([^:]+): (.*)$', stripped_line)
            if search_object:
                configuration_dict[search_object.group(1)] = search_object.group(2)
    return configuration_dict


def parse_file(configuration_dict_source_file_path):
    """
    File parsing worker function
    :param configuration_dict_source_file_path: List or tuple with 2 elements: configuration_dict and source_file_path
    :return: dictionary that tells what to write
    """
    configuration_dict = configuration_dict_source_file_path[0]
    source_file_path = configuration_dict_source_file_path[1]
    result_dict = {key: [] for key in configuration_dict}
    with open(source_file_path) as file:
        try:
            for line in file:
                stripped_line = line.rstrip('\r\n')
                for key in configuration_dict:
                    if search(configuration_dict[key], stripped_line):
                        result_dict[key].append(stripped_line)
        except Exception as e:
            print('Error parsing {}: {}'.format(source_file_path, e))
            result_dict = {}
    return (result_dict)


def main():
    # Parsing arguments
    argument_parser = ArgumentParser(description='Filters lines from files.', add_help=False)
    argument_parser.add_argument('source_directory', help='Source directory')
    argument_parser.add_argument('target_directory', help='Target directory')
    argument_parser.add_argument('configuration_file_path', help='Configuration file path')
    argument_parser.add_argument('number_of_processes', type=int, help='Number of processes')
    argument_parser.add_argument('number_of_files_in_one_batch', type=int, help='Number of files to process in one batch')
    try:
        args = argument_parser.parse_args()
    except Exception as e:
        print(e)
        argument_parser.print_help(file=stderr)
        exit(1)

    # Creating objects
    configuration_dict = parse_configuration_file(args.configuration_file_path)
    pool = Pool(args.number_of_processes)
    source_file_paths = []
    for source_file_name in listdir(args.source_directory):
        source_file_path = join(args.source_directory, source_file_name)
        if isfile(source_file_path):
            source_file_paths.append(source_file_path)
    file_dict = {}
    for key in configuration_dict:
        file_dict[key] = open(join(args.target_directory, '{}.txt'.format(key)), 'w')

    # Running processes
    i = 0
    while i < len(source_file_paths):
        result_dicts = pool.map(parse_file, [(configuration_dict, source_file_path) for source_file_path in source_file_paths[i:i + args.number_of_files_in_one_batch]])
        for result_dict in result_dicts:
            for key in result_dict:
                file_dict[key].write('\n'.join(result_dict[key]))
        i += args.number_of_files_in_one_batch

    for key in file_dict:
        file_dict[key].close()


if __name__ == '__main__':
    main()
