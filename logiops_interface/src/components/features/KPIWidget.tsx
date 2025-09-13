import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { TrendingUp, TrendingDown, Minus } from "lucide-react";
import { LucideIcon } from "lucide-react";

interface KPIData {
  title: string;
  value: string | number;
  change?: string;
  trend?: "up" | "down" | "stable";
  icon: LucideIcon;
  description?: string;
  color?: string;
  prefix?: string;
  suffix?: string;
}

interface KPIWidgetProps {
  data: KPIData;
  size?: "sm" | "md" | "lg";
  variant?: "default" | "compact";
}

export const KPIWidget = ({ 
  data, 
  size = "md", 
  variant = "default" 
}: KPIWidgetProps) => {
  const { title, value, change, trend, icon: Icon, description, color, prefix, suffix } = data;

  const getTrendIcon = () => {
    switch (trend) {
      case "up": return <TrendingUp className="h-3 w-3" />;
      case "down": return <TrendingDown className="h-3 w-3" />;
      case "stable": return <Minus className="h-3 w-3" />;
      default: return null;
    }
  };

  const getTrendColor = () => {
    switch (trend) {
      case "up": return "text-success";
      case "down": return "text-destructive";
      case "stable": return "text-muted-foreground";
      default: return "text-muted-foreground";
    }
  };

  const getSizeClasses = () => {
    switch (size) {
      case "sm":
        return {
          card: "transition-all duration-200 hover:shadow-md",
          icon: "h-3 w-3",
          value: "text-lg",
          title: "text-xs",
          change: "text-xs"
        };
      case "lg":
        return {
          card: "transition-all duration-300 hover:shadow-xl",
          icon: "h-6 w-6",
          value: "text-4xl",
          title: "text-base",
          change: "text-sm"
        };
      default:
        return {
          card: "transition-all duration-300 hover:shadow-lg",
          icon: "h-4 w-4",
          value: "text-2xl",
          title: "text-sm",
          change: "text-xs"
        };
    }
  };

  const classes = getSizeClasses();

  if (variant === "compact") {
    return (
      <Card className={classes.card}>
        <CardContent className="p-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <div className={`p-2 rounded-lg bg-primary/10 ${color || 'text-primary'}`}>
                <Icon className={classes.icon} />
              </div>
              <div>
                <div className={`font-bold ${color || 'text-primary'} ${classes.value}`}>
                  {prefix}{value}{suffix}
                </div>
                <div className={`font-medium text-muted-foreground ${classes.title}`}>
                  {title}
                </div>
              </div>
            </div>
            {change && (
              <div className={`flex items-center space-x-1 ${getTrendColor()} ${classes.change}`}>
                {getTrendIcon()}
                <span>{change}</span>
              </div>
            )}
          </div>
          {description && (
            <div className="text-xs text-muted-foreground mt-2">
              {description}
            </div>
          )}
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className={classes.card}>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className={`font-medium text-muted-foreground ${classes.title}`}>
          {title}
        </CardTitle>
        <Icon className={`${classes.icon} text-muted-foreground`} />
      </CardHeader>
      <CardContent>
        <div className={`font-bold ${color || 'text-primary'} ${classes.value}`}>
          {prefix}{value}{suffix}
        </div>
        {change && (
          <div className={`flex items-center mt-1 ${getTrendColor()} ${classes.change}`}>
            {getTrendIcon()}
            <span className="ml-1">{change}</span>
          </div>
        )}
        {description && (
          <div className="text-xs text-muted-foreground mt-2">
            {description}
          </div>
        )}
      </CardContent>
    </Card>
  );
};