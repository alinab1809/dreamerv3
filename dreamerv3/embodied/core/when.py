import time


class Every:

  def __init__(self, every, initial=True):
    self._every = every
    self._initial = initial
    self._prev = None

  def __call__(self, step):
    step = int(step)
    if self._every < 0:
      return True
    if self._every == 0:
      return False
    if self._prev is None:
      self._prev = (step // self._every) * self._every
      return self._initial
    if step >= self._prev + self._every:
      self._prev += self._every
      return True
    return False


class Ratio:

  def __init__(self, ratio):
    assert ratio >= 0, ratio
    print('ratio ', ratio)
    self._ratio = ratio
    self._prev = None

  def __call__(self, step):
    step = int(step)
    if self._ratio == 0:
      return 0
    if self._prev is None:
      self._prev = step
      return 1
    repeats = int((step - self._prev) * self._ratio)
    print('reps ', repeats)
    self._prev += repeats / self._ratio
    print('new prev ', self._prev)
    return repeats

class Sinnvoll:

  def __init__(self, batch_size, ratio):
    assert ratio >= 0, ratio
    # idea: initially batch_size 8, then 1 gradient step -- now bigger batches (train less often), still keep ratio
    self._batch_size = batch_size
    self._training_steps = int(batch_size / ratio)
    print(f'Doing {self._training_steps} training steps every {self._batch_size} environment steps')
    self._prev = None

  def __call__(self, step):
    step = int(step)
    # print("STEP ", step, "prev ", self._prev)
    if self._prev is None:
        # we need 1 training step in the beginning for an initial report
        self._prev = step
        return 1
    elif step - self._prev == self._batch_size:
        self._prev = step
        return self._training_steps
    return 0



class Once:

  def __init__(self):
    self._once = True

  def __call__(self):
    if self._once:
      self._once = False
      return True
    return False


class Until:

  def __init__(self, until):
    self._until = until

  def __call__(self, step):
    step = int(step)
    if not self._until:
      return True
    return step < self._until


class Clock:

  def __init__(self, every):
    self._every = every
    self._prev = None

  def __call__(self, step=None):
    if self._every < 0:
      return True
    if self._every == 0:
      return False
    now = time.time()
    if self._prev is None:
      self._prev = now
      return True
    if now >= self._prev + self._every:
      # self._prev += self._every
      self._prev = now
      return True
    return False
