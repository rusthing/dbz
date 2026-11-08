/**
 * 少量选项的选择控件（按钮组）。
 *
 * 规范见 dtw-web/docs/ui-spec.md：固定枚举 2–5 个用按钮组，≥6 仍用下拉；
 * 选中项强调色描边 + 浅强调底，未选中为中性描边；容器 `radiogroup`，每项 `radio`。
 */
import { type KeyboardEvent } from 'react';

export interface OptionButtonsOption<T extends string | number = number> {
  value: T;
  label: string;
  disabled?: boolean;
}

export default function OptionButtons<T extends string | number = number>({
  options,
  value,
  onChange,
  ariaLabel,
  allowClear = false,
  clearLabel = '全部',
  disabled = false,
  className,
}: {
  options: OptionButtonsOption<T>[];
  value?: T;
  onChange: (value: T | undefined) => void;
  ariaLabel: string;
  /** 筛选场景显示「全部」项，点击后回传 undefined。 */
  allowClear?: boolean;
  clearLabel?: string;
  disabled?: boolean;
  className?: string;
}) {
  const entries: Array<OptionButtonsOption<T> | { value: undefined; label: string }> = allowClear
    ? [{ value: undefined, label: clearLabel }, ...options]
    : options;

  const move = (delta: number | 'home' | 'end') => {
    const enabled = entries.filter((entry) => !('disabled' in entry && entry.disabled));
    if (enabled.length === 0) return;
    const currentIndex = enabled.findIndex((entry) => entry.value === value);
    let nextIndex = currentIndex < 0 ? 0 : currentIndex;
    if (delta === 'home') nextIndex = 0;
    else if (delta === 'end') nextIndex = enabled.length - 1;
    else nextIndex = (nextIndex + delta + enabled.length) % enabled.length;
    onChange(enabled[nextIndex].value as T | undefined);
  };

  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (disabled) return;
    if (event.key === 'ArrowRight' || event.key === 'ArrowDown') {
      event.preventDefault();
      move(1);
    } else if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') {
      event.preventDefault();
      move(-1);
    } else if (event.key === 'Home') {
      event.preventDefault();
      move('home');
    } else if (event.key === 'End') {
      event.preventDefault();
      move('end');
    }
  };

  return (
    <div
      role="radiogroup"
      aria-label={ariaLabel}
      aria-disabled={disabled || undefined}
      onKeyDown={onKeyDown}
      className={className ? `dbz-option-buttons ${className}` : 'dbz-option-buttons'}
    >
      {entries.map((entry) => {
        const active = entry.value === value;
        const optionDisabled = disabled || ('disabled' in entry && Boolean(entry.disabled));
        const key = entry.value === undefined ? '__all__' : String(entry.value);
        return (
          <button
            key={key}
            type="button"
            role="radio"
            aria-checked={active}
            disabled={optionDisabled}
            tabIndex={active ? 0 : -1}
            onClick={() => onChange(entry.value as T | undefined)}
            className="dbz-option-button"
          >
            {entry.label}
          </button>
        );
      })}
    </div>
  );
}
