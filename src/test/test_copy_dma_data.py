from tasks import  task_dma_data_copy

# task_dma_data_copy.start(_from='2019-05-27', to='2019-05-28', just_scada=True)
# task_dma_data_copy.start(_from='2019-05-28', to='2019-05-29', just_scada=True)
# task_dma_data_copy.start(_from='2019-05-29', to='2019-05-30', just_scada=True)
# task_dma_data_copy.start(_from='2019-05-30', to='2019-05-31')
# task_dma_data_copy.start(_from='2019-05-31', to='2019-06-01')
# task_dma_data_copy.start(_from='2019-06-01', to='2019-06-02')

# task_dma_data_copy.start(_from='2019-06-02', to='2019-06-03', just_scada=True)
# task_dma_data_copy.start(_from='2019-06-03', to='2019-06-04', just_scada=True)

task_dma_data_copy.start()

# from data import realtime
#
# al = realtime.items()
# for k, v in al.items():
#     if not isinstance(v, dict):
#         continue
#     _id = v['_id']
#     t = v['time']
#     vl = v['value']
#     if isinstance(vl, dict):
#         vl = next(iter(vl.items()))[1]
#     # realtime.remove(_id)
#     realtime.upsert(_id, vl, t)
