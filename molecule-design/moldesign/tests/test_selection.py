from moldesign.select import random_selection, greedy_selection

example_tasks = [{'input': 1, 'output': 2}, {'input': 2, 'output': 1}]


def test_random():
    s = random_selection(example_tasks, 1)
    assert len(s) == 1
    assert isinstance(s[0], dict)


def test_greedy():
    s = greedy_selection(example_tasks, 2, lambda x: -1 * x['output'])
    assert len(s) == 2
    assert s[0]['output'] == 2
