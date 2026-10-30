"""Explicit CYQUARK reconstruction preparation and optional single-GPU SFT.

Importing, auditing, or building examples never downloads models or trains.
The input must be separately generated, paraphrased and filtered training data.
"""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import re


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode('utf8')).hexdigest()


def paraphrases(text):
    """The public generator template requests exactly three numbered answers."""
    match = re.fullmatch(r'\s*1\)\s*(.+?)\s+2\)\s*(.+?)\s+3\)\s*(.+?)\s*\[END\]\s*', text, re.S)
    if not match:
        raise ValueError('Malformed three-paraphrase response; retain raw response for audit')
    result = [part.strip() for part in match.groups()]
    if len(set(result)) != 3:
        raise ValueError('Duplicate paraphrases')
    return result


def filter_result(text):
    decisions = re.findall(r'<sense>\s*(yes|no)\s*</sense>', text, re.I)
    if len(decisions) != 1:
        raise ValueError('Missing or ambiguous logical-filter decision')
    return decisions[0].lower() == 'yes'


def checked_rows(path, *, benchmark, graph, heldout_questions=()):
    """Require graph-bound provenance. No test answer is needed for exclusion."""
    seen, rows, rejected = set(), [], []
    normalize = lambda s: ' '.join(s.split()).casefold()
    heldout = {normalize(s) for s in heldout_questions}
    with Path(path).open(encoding='utf-8-sig') as stream:
        for line_number, line in enumerate(stream, 1):
            row = json.loads(line)
            if (row.get('benchmark'),row.get('graph'),row.get('split')) != (benchmark,graph,'train'):
                raise ValueError('Wrong graph or split at line ' + str(line_number))
            if not row.get('source_id') or row['source_id'] in seen:
                raise ValueError('Missing or duplicate source identity')
            seen.add(row['source_id'])
            if not isinstance(row.get('messages'), list) or not row['messages'] or not row.get('cypher') or not row.get('question'):
                raise ValueError('Training record needs messages, question and Cypher')
            if any(m.get('role') not in ('system','user') or not isinstance(m.get('content'),str) for m in row['messages']):
                raise ValueError('Training prompt may contain only public system/user inputs')
            provenance = row.get('provenance', {})
            if not all(provenance.get(k) for k in ('generator_revision','schema_sha256','snapshot_sha256',
                                                   'paraphrase_response_sha256','filter_response_sha256','prompt_profile_sha256')):
                raise ValueError('Training provenance is incomplete')
            if row.get('logical_filter_passed') is not True:
                rejected.append(dict(source_id=row['source_id'], reason='logical_filter_not_passed'))
            elif normalize(row['question']) in heldout:
                rejected.append(dict(source_id=row['source_id'], reason='exact_heldout_question_overlap'))
            else:
                rows.append(row)
    return rows, rejected


def encode_example(tokenizer, row, max_length):
    """Tokenize complete chats; reject a template whose response boundary differs."""
    options = dict(tokenize=True, enable_thinking=False)
    prefix = tokenizer.apply_chat_template(row['messages'], add_generation_prompt=True, **options)
    full = tokenizer.apply_chat_template(row['messages'] + [dict(role='assistant', content=row['cypher'])],
                                         add_generation_prompt=False, **options)
    if full[:len(prefix)] != prefix or len(full) <= len(prefix):
        raise ValueError('Tokenizer/template completion boundary mismatch')
    if len(full) > max_length:
        return None
    return dict(input_ids=full, attention_mask=[1]*len(full), labels=[-100]*len(prefix)+full[len(prefix):])


def author_tsv_rows(path, *, benchmark, graph, source, heldout_questions=()):
    """Import supplied corpora without inventing generation/filtering provenance."""
    if (source.get('benchmark'), source.get('graph')) != (benchmark, graph):
        raise ValueError('Author source graph binding mismatch')
    for field in ('archive_sha256', 'member_sha256', 'member_path', 'permission_evidence_sha256'):
        if not source.get(field):
            raise ValueError('Incomplete author source receipt')
    verify_file(path, source['member_sha256'])
    heldout = {' '.join(q.split()).casefold() for q in heldout_questions}
    rows, rejected = [], []
    with Path(path).open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.DictReader(stream, delimiter='\t')
        if reader.fieldnames != ['X', 'Y']:
            raise ValueError('Unexpected author TSV columns')
        for index, record in enumerate(reader, 1):
            if None in record or not record['X'] or not record['Y']:
                raise ValueError('Malformed author TSV record ' + str(index))
            identity = source['member_sha256'] + ':' + str(index)
            question = record['X']
            if ' '.join(question.split()).casefold() in heldout:
                rejected.append(dict(source_id=identity, reason='exact_heldout_question_overlap'))
                continue
            rows.append(dict(source_id=identity, benchmark=benchmark, graph=graph,
                             split='train', question=question, cypher=record['Y'],
                             messages=[dict(role='user', content=question)],
                             provenance=dict(kind='author_supplied', source=source, record_index=index)))
    return rows, rejected


def validate_recipe(recipe):
    required = ('benchmark','graph','model_path','tokenizer_path','dataset_path','dataset_sha256',
                'heldout_questions_path','heldout_questions_sha256','model_config_sha256',
                'tokenizer_config_sha256','epochs','learning_rate','weight_decay','effective_batch_size',
                'micro_batch_size','max_length','seed','output_dir','choices')
    if any(recipe.get(key) is None for key in required):
        raise ValueError('Incomplete explicit training recipe')
    if (recipe['learning_rate'],recipe['weight_decay'],recipe['effective_batch_size']) != (5e-6,0.01,16):
        raise ValueError('Version any alternative learning rate, decay or effective batch')
    training_options(recipe)
    if type(recipe['micro_batch_size']) is not int or recipe['micro_batch_size'] < 1 or 16 % recipe['micro_batch_size']:
        raise ValueError('Micro batch must divide effective batch 16')
    if type(recipe['max_length']) is not int or recipe['max_length'] < 1 or type(recipe['seed']) is not int:
        raise ValueError('Explicit positive sequence limit and integer seed required')
    if recipe['choices'].get('loss_scope') != 'completion_only' or recipe['choices'].get('thinking') is not False:
        raise ValueError('Freeze the disclosed completion-only, non-thinking engineering profile')


def training_options(recipe):
    """Preserve historical recipes; require explicit evidence for a new profile."""
    profile = recipe.get('training_profile', 'legacy_v28')
    if profile == 'legacy_v28':
        if recipe['epochs'] != 1:
            raise ValueError('Legacy profile requires one epoch')
        return dict(optim='adamw_torch', lr_scheduler_type='constant')
    if profile != 'explicit_v2':
        raise ValueError('Unknown training profile')
    if not recipe.get('settings_evidence_sha256'):
        raise ValueError('Explicit profile requires settings evidence')
    if type(recipe['epochs']) is not int or recipe['epochs'] < 1:
        raise ValueError('Epochs must be an explicit positive integer')
    optimizer = recipe.get('optimizer')
    scheduler = recipe.get('lr_scheduler_type')
    if optimizer not in ('adafactor', 'adamw_torch') or scheduler not in ('cosine', 'constant'):
        raise ValueError('Unsupported explicit optimizer or scheduler')
    return dict(optim=optimizer, lr_scheduler_type=scheduler)


def verify_file(path, expected):
    if hashlib.sha256(Path(path).read_bytes()).hexdigest() != expected:
        raise ValueError('Changed training material: ' + str(path))


def train(recipe):
    """Single GPU deliberately avoids implicit distributed/checkpoint ownership."""
    validate_recipe(recipe)
    if recipe.get('training_authorized') is not True:
        raise ValueError('Preparation is not training authorization')
    if int(os.environ.get('WORLD_SIZE','1')) != 1:
        raise ValueError('This frozen recipe is single-GPU; parallelize independent graph jobs explicitly')
    for path,sha in ((recipe['dataset_path'],recipe['dataset_sha256']),
                     (recipe['heldout_questions_path'],recipe['heldout_questions_sha256']),
                     (Path(recipe['model_path'])/'config.json',recipe['model_config_sha256']),
                     (Path(recipe['tokenizer_path'])/'tokenizer_config.json',recipe['tokenizer_config_sha256'])):
        verify_file(path,sha)
    heldout = json.loads(Path(recipe['heldout_questions_path']).read_text(encoding='utf8'))
    data_format = recipe.get('dataset_format', 'generated_jsonl')
    if data_format == 'author_tsv_v1':
        rows, rejected = author_tsv_rows(recipe['dataset_path'], benchmark=recipe['benchmark'],
                                        graph=recipe['graph'], source=recipe['author_source'],
                                        heldout_questions=heldout)
    elif data_format == 'generated_jsonl':
        rows, rejected = checked_rows(recipe['dataset_path'], benchmark=recipe['benchmark'],
                                      graph=recipe['graph'], heldout_questions=heldout)
    else:
        raise ValueError('Unknown training dataset format')
    # Heavy imports happen only after explicit authorization and file checks.
    import torch
    from transformers import AutoTokenizer, Qwen3_5ForConditionalGeneration, Trainer, TrainingArguments, DataCollatorForSeq2Seq
    if not torch.cuda.is_available():
        raise ValueError('GPU unavailable; do not silently start a long CPU training run')
    if torch.cuda.device_count() != 1:
        raise ValueError('Expose exactly one GPU with CUDA_VISIBLE_DEVICES to preserve effective batch 16')
    if not torch.cuda.is_bf16_supported():
        raise ValueError('This recipe requires BF16; do not silently change precision')
    output=Path(recipe['output_dir'])
    output.mkdir(parents=True,exist_ok=False)
    tokenizer=AutoTokenizer.from_pretrained(recipe['tokenizer_path'],local_files_only=True,trust_remote_code=False)
    examples=[]
    for row in rows:
        value=encode_example(tokenizer,row,recipe['max_length'])
        if value is None:
            rejected.append(dict(source_id=row['source_id'],reason='sequence_too_long_no_truncation'))
        else:
            examples.append(value)
    (output/'preprocessing.json').write_text(json.dumps(dict(accepted=len(examples),rejected=rejected),indent=2),encoding='utf8')
    if not examples:
        raise ValueError('No accepted training examples')
    args=TrainingArguments(output_dir=str(output/'checkpoints'),num_train_epochs=recipe['epochs'],
         learning_rate=recipe['learning_rate'],weight_decay=recipe['weight_decay'],
         per_device_train_batch_size=recipe['micro_batch_size'],gradient_accumulation_steps=16//recipe['micro_batch_size'],
         bf16=True,seed=recipe['seed'],data_seed=recipe['seed'],**training_options(recipe),
         warmup_ratio=0,gradient_checkpointing=True,report_to=[],save_strategy='epoch',save_total_limit=1)
    model=Qwen3_5ForConditionalGeneration.from_pretrained(recipe['model_path'],local_files_only=True,
                                                       trust_remote_code=False,torch_dtype=torch.bfloat16)
    model.config.use_cache=False
    trainer=Trainer(model=model,args=args,train_dataset=examples,
                    data_collator=DataCollatorForSeq2Seq(tokenizer,model=None,padding=True,label_pad_token_id=-100))
    trainer.train()
    trainer.save_model(str(output/'model'))
    tokenizer.save_pretrained(output/'model')
    import importlib.metadata
    receipt=dict(recipe=recipe,recipe_sha256=fingerprint(recipe),accepted_examples=len(examples),
                 packages={p:importlib.metadata.version(p) for p in ('torch','transformers','accelerate')},
                 label='our_reimplementation_not_author_checkpoint')
    (output/'training_receipt.json').write_text(json.dumps(receipt,indent=2),encoding='utf8')


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=('audit','train'))
    parser.add_argument('recipe',type=Path)
    args=parser.parse_args()
    recipe=json.loads(args.recipe.read_text(encoding='utf-8-sig'))
    validate_recipe(recipe)
    if args.command=='train': train(recipe)
    else: print(json.dumps(dict(recipe_valid=True,training_runs=0,GPU_checked=False)))
