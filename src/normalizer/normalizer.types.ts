export type EventKind = 'MESSAGE' | 'TEMPLATE' | 'USER' | 'UNKNOWN';

export type MessageStatus = 'accepted' | 'sent' | 'delivered' | 'read' | 'failed';

export type TemplateStatus = 'APPROVED' | 'REJECTED' | 'PENDING' | 'SUBMITTED';

export type ConsentEvent = 'OPT_IN' | 'OPT_OUT' | 'BLOCKED';

export interface NormalizedMessageEvent {
  kind: 'MESSAGE';
  providerEventId: string | null;
  messageId: string | null;
  whatsappMessageId: string | null;
  status: MessageStatus | null;
  eventAt: Date | null;
  errorCode: string | null;
  errorReason: string | null;
  errorPayload: Record<string, unknown> | null;
}

export interface NormalizedTemplateEvent {
  kind: 'TEMPLATE';
  providerEventId: string | null;
  templateName: string | null;
  templateProviderId: string | null;
  templateStatus: TemplateStatus | null;
  language: string | null;
  rejectionReason: string | null;
  correctCategory: string | null;
  eventAt: Date | null;
}

export interface NormalizedUserEvent {
  kind: 'USER';
  providerEventId: string | null;
  phone: string | null;
  consentEvent: ConsentEvent | null;
  eventAt: Date | null;
}

export interface NormalizedUnknownEvent {
  kind: 'UNKNOWN';
  providerEventId: string | null;
  eventAt: Date | null;
}

export type NormalizedWebhookEvent =
  | NormalizedMessageEvent
  | NormalizedTemplateEvent
  | NormalizedUserEvent
  | NormalizedUnknownEvent;
