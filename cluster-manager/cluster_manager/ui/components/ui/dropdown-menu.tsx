import { useEffect, useRef, useState } from "react";
import { cn } from "../../lib/utils";

interface DropdownMenuProps {
  trigger: React.ReactNode;
  children: React.ReactNode;
  align?: "left" | "right";
}

export function DropdownMenu({ trigger, children, align = "right" }: DropdownMenuProps) {
  const [isOpen, setIsOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    }

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  return (
    <div ref={menuRef} className="relative inline-block">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="p-1.5 rounded-md hover:bg-muted transition-colors inline-flex items-center justify-center"
      >
        {trigger}
      </button>
      {isOpen && (
        <div
          className={cn(
            "absolute z-50 mt-1 min-w-[180px] rounded-md border bg-popover p-1 shadow-md",
            align === "right" ? "right-0" : "left-0"
          )}
        >
          {children}
        </div>
      )}
    </div>
  );
}

interface DropdownMenuItemProps {
  children: React.ReactNode;
  onClick?: () => void;
  href?: string;
  external?: boolean;
  icon?: React.ReactNode;
  disabled?: boolean;
}

export function DropdownMenuItem({
  children,
  onClick,
  href,
  external,
  icon,
  disabled,
}: DropdownMenuItemProps) {
  const baseClasses = cn(
    "flex w-full items-center gap-2 rounded-sm px-2 py-1.5 text-sm",
    disabled
      ? "opacity-50 cursor-not-allowed"
      : "cursor-pointer hover:bg-accent hover:text-accent-foreground"
  );

  if (href && !disabled) {
    return (
      <a
        href={href}
        target={external ? "_blank" : undefined}
        rel={external ? "noopener noreferrer" : undefined}
        className={baseClasses}
        onClick={onClick}
      >
        {icon}
        {children}
        {external && (
          <svg
            className="ml-auto h-3 w-3 opacity-50"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"
            />
          </svg>
        )}
      </a>
    );
  }

  return (
    <button onClick={onClick} disabled={disabled} className={baseClasses}>
      {icon}
      {children}
    </button>
  );
}

export function DropdownMenuSeparator() {
  return <div className="my-1 h-px bg-border" />;
}

export function DropdownMenuLabel({ children }: { children: React.ReactNode }) {
  return (
    <div className="px-2 py-1.5 text-xs font-semibold text-muted-foreground">
      {children}
    </div>
  );
}
