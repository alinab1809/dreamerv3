import re
from collections import defaultdict
from functools import partial as bind

import embodied
import numpy as np


def eval_only(make_agent, make_env, make_eval_replay, make_logger, args):
  assert args.from_checkpoint

  agent = make_agent()
  eval_replay = make_eval_replay()
  logger = make_logger()

  logdir = embodied.Path(args.logdir)
  logdir.mkdir()
  print('Logdir', logdir)
  step = logger.step
  usage = embodied.Usage(**args.usage)
  agg = embodied.Agg()
  eval_epstats = embodied.Agg()
  episodes = defaultdict(embodied.Agg)
  should_log = embodied.when.Clock(args.log_every)
  policy_fps = embodied.FPS()
  carry = [agent.init_train(args.batch_size)]
  carry_report = agent.init_report(args.batch_size)

  @embodied.timer.section('log_step')
  def log_step(tran, worker):

    episode = episodes[worker]
    episode.add('score', tran['reward'], agg='sum')
    episode.add('length', 1, agg='sum')
    episode.add('rewards', tran['reward'], agg='stack')

    if tran['is_first']:
      episode.reset()

    if worker < args.log_video_streams:
      for key in args.log_keys_video:
        if key in tran:
          episode.add(f'policy_{key}', tran[key], agg='stack')
    for key, value in tran.items():
      if re.match(args.log_keys_sum, key):
        episode.add(key, value, agg='sum')
      if re.match(args.log_keys_avg, key):
        episode.add(key, value, agg='avg')
      if re.match(args.log_keys_max, key):
        episode.add(key, value, agg='max')

    if tran['is_last']:
      result = episode.result()
      logger.add({
          'score': result.pop('score'),
          'length': result.pop('length') - 1,
      }, prefix='episode')
      rew = result.pop('rewards')
      if len(rew) > 1:
        result['reward_rate'] = (np.abs(rew[1:] - rew[:-1]) >= 0.01).mean()
      eval_epstats.add(result)

  fns = [bind(make_env, i) for i in range(args.num_envs)]
  driver = embodied.Driver(fns, args.driver_parallel)
  # driver.on_step(lambda tran, _: step.increment())
  driver.on_step(lambda tran, _: policy_fps.step())
  driver.on_step(log_step)

  driver.on_step(eval_replay.add)
  # driver.on_step(bind(log_step, mode='eval'))
  dataset_eval = agent.dataset(
    bind(eval_replay.dataset, args.batch_size, args.batch_length_eval))
  logdir = embodied.Path(args.logdir)
  checkpoint = embodied.Checkpoint(logdir / 'checkpoint.ckpt')
  checkpoint.agent = agent
  checkpoint.eval_replay = eval_replay
  checkpoint.load(args.from_checkpoint, keys=['agent'])

  print('Start evaluation')
  policy = lambda *args: agent.policy(*args, mode='eval')
  driver.reset(agent.init_policy)
  # while step < args.steps:
  #   driver(policy, steps=10)
  #   if should_log(step):
  #     logger.add(agg.result())
  #     logger.add(epstats.result(), prefix='epstats')
  #     logger.add(embodied.timer.stats(), prefix='timer')
  #     logger.add(usage.stats(), prefix='usage') # TODO: what is this?
  #     logger.add({'fps/policy': policy_fps.result()})
  #     logger.write()

  driver(policy, episodes=args.eval_eps)
  logger.add(eval_epstats.result(), prefix='epstats')

  if len(eval_replay):
    mets, _ = agent.report(next(dataset_eval), carry_report)
    logger.add(mets, prefix='eval')
  logger.write()
  checkpoint.save('eval')
  #  logger.close()  # TODO: do we need this to log during training again?
