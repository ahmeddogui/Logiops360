import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { 
  AlertTriangle, 
  AlertCircle, 
  Info, 
  CheckCircle,
  Clock,
  X
} from "lucide-react";

interface Alert {
  id: string;
  type: "error" | "warning" | "info" | "success";
  title: string;
  message: string;
  timestamp: string;
  module: "commandes" | "stockage" | "transport" | "system";
  severity: "low" | "medium" | "high" | "critical";
  acknowledged?: boolean;
}

interface AlertsPanelProps {
  alerts: Alert[];
  onAcknowledge?: (alertId: string) => void;
  onDismiss?: (alertId: string) => void;
  maxHeight?: number;
}

export const AlertsPanel = ({ 
  alerts, 
  onAcknowledge, 
  onDismiss,
  maxHeight = 400 
}: AlertsPanelProps) => {
  
  const getAlertIcon = (type: Alert["type"]) => {
    switch (type) {
      case "error": return <AlertCircle className="h-4 w-4" />;
      case "warning": return <AlertTriangle className="h-4 w-4" />;
      case "info": return <Info className="h-4 w-4" />;
      case "success": return <CheckCircle className="h-4 w-4" />;
    }
  };

  const getAlertColor = (type: Alert["type"]) => {
    switch (type) {
      case "error": return "text-destructive";
      case "warning": return "text-warning";
      case "info": return "text-primary";
      case "success": return "text-success";
    }
  };

  const getSeverityColor = (severity: Alert["severity"]) => {
    switch (severity) {
      case "critical": return "destructive";
      case "high": return "destructive";
      case "medium": return "default";
      case "low": return "secondary";
    }
  };

  const getModuleColor = (module: Alert["module"]) => {
    switch (module) {
      case "commandes": return "outline";
      case "stockage": return "outline";
      case "transport": return "outline";
      case "system": return "secondary";
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center space-x-2">
          <AlertTriangle className="h-5 w-5" />
          <span>Alertes système</span>
          <Badge variant="outline" className="ml-auto">
            {alerts.filter(a => !a.acknowledged).length} nouvelles
          </Badge>
        </CardTitle>
        <CardDescription>
          Notifications en temps réel de la supply chain
        </CardDescription>
      </CardHeader>
      
      <CardContent>
        <ScrollArea className={`w-full`} style={{ height: `${maxHeight}px` }}>
          <div className="space-y-3">
            {alerts.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground">
                <CheckCircle className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p>Aucune alerte active</p>
                <p className="text-sm">Tous les systèmes fonctionnent normalement</p>
              </div>
            ) : (
              alerts.map((alert) => (
                <div 
                  key={alert.id} 
                  className={`p-3 border rounded-lg transition-all duration-200 ${
                    alert.acknowledged ? 'opacity-60' : 'opacity-100'
                  }`}
                >
                  <div className="flex items-start justify-between mb-2">
                    <div className="flex items-start space-x-3">
                      <div className={`${getAlertColor(alert.type)} mt-0.5`}>
                        {getAlertIcon(alert.type)}
                      </div>
                      <div className="flex-1">
                        <div className="flex items-center space-x-2 mb-1">
                          <h4 className="font-medium text-sm">{alert.title}</h4>
                          <Badge variant={getSeverityColor(alert.severity)} className="text-xs">
                            {alert.severity}
                          </Badge>
                          <Badge variant={getModuleColor(alert.module)} className="text-xs">
                            {alert.module}
                          </Badge>
                        </div>
                        <p className="text-sm text-muted-foreground">{alert.message}</p>
                        <div className="flex items-center space-x-2 mt-2 text-xs text-muted-foreground">
                          <Clock className="h-3 w-3" />
                          <span>{alert.timestamp}</span>
                        </div>
                      </div>
                    </div>
                    
                    <div className="flex space-x-1">
                      {!alert.acknowledged && onAcknowledge && (
                        <Button 
                          size="sm" 
                          variant="outline"
                          onClick={() => onAcknowledge(alert.id)}
                          className="h-6 px-2"
                        >
                          <CheckCircle className="h-3 w-3" />
                        </Button>
                      )}
                      {onDismiss && (
                        <Button 
                          size="sm" 
                          variant="ghost"
                          onClick={() => onDismiss(alert.id)}
                          className="h-6 px-2"
                        >
                          <X className="h-3 w-3" />
                        </Button>
                      )}
                    </div>
                  </div>
                  
                  {alert.acknowledged && (
                    <div className="text-xs text-success flex items-center space-x-1">
                      <CheckCircle className="h-3 w-3" />
                      <span>Prise en compte</span>
                    </div>
                  )}
                </div>
              ))
            )}
          </div>
        </ScrollArea>
        
        {alerts.length > 0 && (
          <div className="mt-4 pt-4 border-t">
            <Button variant="outline" className="w-full">
              Voir l'historique des alertes
            </Button>
          </div>
        )}
      </CardContent>
    </Card>
  );
};