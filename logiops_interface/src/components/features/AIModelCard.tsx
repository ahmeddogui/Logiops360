import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Bot, Play, Pause, RefreshCw } from "lucide-react";

interface AIModel {
  id: string;
  name: string;
  description: string;
  endpoint: string;
  status: "active" | "inactive" | "training" | "error";
  accuracy: number;
  lastRun: string;
  nextRun?: string;
}

interface AIModelCardProps {
  model: AIModel;
  onTrigger: (modelId: string) => void;
  onStop?: (modelId: string) => void;
}

export const AIModelCard = ({ model, onTrigger, onStop }: AIModelCardProps) => {
  const getStatusVariant = (status: string) => {
    switch (status) {
      case "active": return "default";
      case "training": return "secondary";
      case "error": return "destructive";
      default: return "outline";
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case "active": return "text-success";
      case "training": return "text-warning";
      case "error": return "text-destructive";
      default: return "text-muted-foreground";
    }
  };

  return (
    <Card className="transition-all duration-300 hover:shadow-lg">
      <CardHeader className="space-y-1">
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg flex items-center space-x-2">
            <Bot className="h-5 w-5" />
            <span>{model.name}</span>
          </CardTitle>
          <Badge variant={getStatusVariant(model.status)}>
            {model.status}
          </Badge>
        </div>
        <CardDescription className="text-sm">
          {model.description}
        </CardDescription>
      </CardHeader>
      
      <CardContent className="space-y-4">
        {/* Métriques de performance */}
        <div className="space-y-2">
          <div className="flex justify-between text-sm">
            <span>Précision</span>
            <span className="font-medium">{model.accuracy}%</span>
          </div>
          <Progress value={model.accuracy} className="h-2" />
        </div>

        {/* Informations temporelles */}
        <div className="space-y-1 text-sm text-muted-foreground">
          <div>Dernière exécution: {model.lastRun}</div>
          {model.nextRun && (
            <div>Prochaine exécution: {model.nextRun}</div>
          )}
        </div>

        {/* Actions */}
        <div className="flex space-x-2">
          <Button 
            size="sm" 
            onClick={() => onTrigger(model.id)}
            disabled={model.status === "training"}
            className="flex-1"
          >
            {model.status === "training" ? (
              <>
                <RefreshCw className="h-3 w-3 mr-1 animate-spin" />
                Entraînement
              </>
            ) : (
              <>
                <Play className="h-3 w-3 mr-1" />
                Exécuter
              </>
            )}
          </Button>
          
          {onStop && model.status === "active" && (
            <Button 
              size="sm" 
              variant="outline"
              onClick={() => onStop(model.id)}
            >
              <Pause className="h-3 w-3" />
            </Button>
          )}
        </div>
      </CardContent>
    </Card>
  );
};