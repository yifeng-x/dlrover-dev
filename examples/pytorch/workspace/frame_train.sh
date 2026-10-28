
#==========================
# grpo
#==========================

swift rlhf --fsdp fsdp2 \
    --rlhf_type grpo \
    --model '/data-disk-nas-disk2/model/Qwen3.5-2B' \
    --model_type 'qwen3_5' \
    --reward_funcs accuracy format \
    --tuner_type lora \
    --use_vllm true \
    --vllm_mode colocate \
    --vllm_gpu_memory_utilization 0.35 \
    --vllm_tensor_parallel_size 1 \
    --vllm_max_model_len 512 \
    --torch_dtype bfloat16 \
    --dataset '/data-disk-nas-disk2/dataset/NuminaMath-TIR.jsonl' \
    --load_from_cache_file true \
    --max_completion_length 1024 \
    --max_step 5 \
    --per_device_train_batch_size 2 \
    --per_device_eval_batch_size 1 \
    --gradient_checkpointing false \
    --learning_rate 1e-5 \
    --eval_steps 100 \
    --save_steps 100 \
    --logging_steps 1 \
    --max_length 512 \
    --output_dir /data-disk/output/workspace-test/Qwen3.5-2B/fsdp2/grpo_lora \
    --warmup_ratio 0.05 \
    --num_generations 2 \
    --temperature 0.9 \
    --log_completions true


#==========================
# sft
#==========================

# swift sft --model /data-disk/model/Qwen2.5-3B-Instruct \
#       --dataset /data-disk/dataset/话术质检_版本2_alpaca_data.json \
#       --output_dir /data-disk/output/workspace-test \
#       --model_type qwen2 \
#       --torch_dtype bfloat16 \
#       --tuner_type lora \
#       --num_train_epochs 3 \
#       --warmup_ratio 0.1 \
#       --per_device_train_batch_size 1 \
#       --per_device_eval_batch_size 1 \
#       --learning_rate 1e-4 \
#       --gradient_accumulation_steps 1 \
#       --max_length 1024 \
#       --weight_decay 0.0001 \
#       --lr_scheduler_type cosine \
#       --logging_steps 1 \
#       --save_steps 20 \
#       --save_total_limit 2 \
#       --lora_rank 8 \
#       --lora_dropout 0.05 \
#       --response_prefix '\n' \
#       --dataloader_num_workers 2 \
#       --lora_alpha 32 \
#       --target_modules all-linear \
#       --dataset_num_proc 1 \
#       --model_name 'workspace-test' \
#       --use_flash_ckpt true \
#       --deepspeed /data-disk/deepspeed/zero4.json \
#       --callbacks deepspeed_elastic graceful_exit