"""Explicit wire profiles for future admitted runs; no network or credentials.

The original top-level reasoning profile is preserved. Vendor nested numeric
effort is a different configuration, never silently mixed with the old one.
"""
from copy import deepcopy


def build_model_payload(config, messages, *, max_tokens):
    payload = {'model': config['model'], 'messages': deepcopy(messages),
               'temperature': config['temperature'], 'top_p': config['top_p']}
    if 'chat_template_kwargs' in config:
        if 'reasoning_effort' in config:
            raise ValueError('Choose one explicit reasoning wire profile')
        template = config['chat_template_kwargs']
        if (type(template) is not dict or set(template) != {'thinking', 'reasoning_effort'}
                or type(template['thinking']) is not bool
                or type(template['reasoning_effort']) is not int
                or template['reasoning_effort'] < 0):
            raise ValueError('Expected explicit nested thinking and numeric effort')
        payload['chat_template_kwargs'] = deepcopy(template)
    else:
        payload['reasoning_effort'] = config['reasoning_effort']
    payload['max_tokens'] = max_tokens
    return payload
